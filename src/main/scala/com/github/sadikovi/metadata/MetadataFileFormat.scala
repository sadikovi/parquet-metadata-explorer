package com.github.sadikovi.metadata

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType

import org.apache.parquet.format.{Encoding, KeyValue, Statistics, Util}
import org.apache.parquet.format.converter.SchemaUtil

/**
 * Case class that has some of the FileStatus metrics.
 * This is done to make the code compatible between OSS and Databricks.
 */
case class FileInfo(
    path: String,
    name: String,
    size: Long,
    mtime: Long,
    partitionValues: InternalRow)

/**
 * Abstract metadata file format requires:
 * - spark, SparkSession
 * - level, metadata level with schema
 * - targetPartitionSchema, partition schema for the target table, not for metadata
 */
abstract class MetadataFileFormat(
    @transient val spark: SparkSession,
    @transient val fileIndex: FileIndex,
    val level: MetadataLevel,
    val maxPartitions: Int)
  extends BaseRelation with TableScan with Serializable {

  override def sqlContext: SQLContext = spark.sqlContext

  override def schema: StructType = level.schema

  /** Returns new Hadoop configuration */
  def hadoopConf: Configuration = spark.sessionState.newHadoopConf

  override def buildScan(): RDD[Row] = {
    val partitionSchema = fileIndex.partitionSchema
    val files = allFileInfo
    val rdd = spark.sparkContext.parallelize(files, Math.min(files.length, maxPartitions))
    buildScan(rdd, partitionSchema)
  }

  /** Builds scan from RDD[FileInfo] */
  def buildScan(rdd: RDD[FileInfo], partitionSchema: StructType): RDD[Row]

  /**
   * Returns all files from file index.
   * Metadata is appended, so we don't have to call getFileStatus in `buildScan` method.
   */
  private def allFileInfo: Seq[FileInfo] = {
    // We need all files, don't apply filtering
    // We rely on file index to have the correct metadata!
    fileIndex.listFiles(Nil, Nil).flatMap { partDir =>
      val partValues = partDir.values
      partDir.files.asInstanceOf[Seq[Any]].map { unknown =>
        // NOTE: this is a hack to make the code work for OSS and Databricks
        val status = new SerializableFileStatus(unknown)
        val path = status.getPath
        FileInfo(path.toString, path.getName, status.getLen,
          status.getModificationTime, partValues)
      }
    }
  }

  override def toString(): String = {
    s"${this.getClass.getSimpleName}(level: $level, maxPartitions: $maxPartitions)"
  }
}

object MetadataFileFormat {
  /** Merges partition values with the schema and returns result as Map(partition -> value) */
  def getPartitionMap(
      partitionSchema: StructType,
      partitionValues: InternalRow): Map[String, String] = {
    // TODO: Most of the types should cast to string correctly, there could be problems with dates
    // and timestamps - I am going to address it later
    val parts = partitionSchema.zipWithIndex.map {
      case (field, i) => (field.name, s"${partitionValues.get(i, field.dataType)}")
    }
    parts.toMap
  }

  /** Converts values into Row, unwraps Options */
  def toRow(values: Array[Any]): Row = Row.fromSeq(values.map {
    case Some(v) => v
    case None => null
    case other => other
  })

  /** Converts Parquet statistics into Row */
  def getStatistics(stats: Option[Statistics]): Option[Row] = {
    stats match {
      case Some(stats) =>
        val nulls = if (stats.isSetNull_count) Some(stats.getNull_count) else None
        val distinct = if (stats.isSetDistinct_count) Some(stats.getDistinct_count) else None
        val min_value = if (stats.isSetMin_value) Some(stats.getMin_value) else None
        val max_value = if (stats.isSetMax_value) Some(stats.getMax_value) else None
        val min = if (stats.isSetMin) Some(stats.getMin) else None
        val max = if (stats.isSetMax) Some(stats.getMax) else None

        val values: Array[Any] = Array(
          nulls.map(_.toLong),
          distinct.map(_.toLong),
          min_value.orElse(min).map(_.length.toLong), // print only length
          max_value.orElse(max).map(_.length.toLong) // print only length
        )

        Some(toRow(values))
      case None =>
        None
    }
  }

  /** Converts Parquet metadata into sequence of key-value pairs */
  def getKeyValueMetadata(keyValueList: java.util.List[KeyValue]): Seq[Row] = {
    Option(keyValueList).map { list =>
      list.asScala.map { pair =>
        val key = pair.getKey // required field
        val value = if (pair.isSetValue) Option(pair.getValue) else None // optional
        toRow(Array(key, value))
      }
    }.getOrElse(Nil)
  }
}

////////////////////////////////////////////////////////////////
// Implementations
////////////////////////////////////////////////////////////////

/** File format for file metadata */
class FileMetadataFormat(
    spark: SparkSession,
    fileIndex: FileIndex,
    level: MetadataLevel,
    maxPartitions: Int)
  extends MetadataFileFormat(spark, fileIndex, level, maxPartitions) {

  override def buildScan(rdd: RDD[FileInfo], partitionSchema: StructType): RDD[Row] = {
    rdd.map { file =>
      val partMap = MetadataFileFormat.getPartitionMap(partitionSchema, file.partitionValues)
      // Must confirm to FileLevel schema
      val values = Array(
        file.path,
        file.name,
        file.size,
        file.mtime,
        partMap
      )
      MetadataFileFormat.toRow(values)
    }
  }
}

/** File format for Parquet metadata */
class ParquetMetadataFileFormat(
    spark: SparkSession,
    fileIndex: FileIndex,
    level: MetadataLevel,
    maxPartitions: Int)
  extends MetadataFileFormat(spark, fileIndex, level, maxPartitions) {

  require(
    level == ParquetFileLevel ||
    level == ParquetRowGroupLevel ||
    level == ParquetColumnLevel ||
    level == ParquetPageLevel,
    s"Unsupported $level for $this")

  override def buildScan(rdd: RDD[FileInfo], partitionSchema: StructType): RDD[Row] = {
    val broadcastedHadoopConf =
      spark.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    rdd.flatMap { file =>
      val conf = broadcastedHadoopConf.value.value
      val metadata = ParquetUtils.readParquetMetadata(new Path(file.path), conf, Some(file.size))

      level match {
        case ParquetFileLevel =>
          val partMap = MetadataFileFormat.getPartitionMap(partitionSchema, file.partitionValues)

          // fromParquetSchema handles null input
          val schema = SchemaUtil.fromParquetSchema(metadata.getSchema, metadata.getColumn_orders)
          val num_rows = metadata.getNum_rows
          val created_by = if (metadata.isSetCreated_by) Option(metadata.getCreated_by) else None
          val key_value_metadata = if (metadata.isSetKey_value_metadata)
            MetadataFileFormat.getKeyValueMetadata(metadata.getKey_value_metadata) else None

          // Must confirm to FileLevel schema
          val values = Array(
            file.path,
            file.name,
            file.size,
            file.mtime,
            partMap,
            Option(schema).map(_.toString),
            num_rows, // required Thrift field
            created_by, // optional
            key_value_metadata // optional
          )
          Iterator(MetadataFileFormat.toRow(values))
        case ParquetRowGroupLevel =>
          metadata.getRow_groups().asScala.zipWithIndex.map { case (rg, id) =>
            val size = rg.getColumns().asScala.map { cc =>
              val cc_meta = if (cc.isSetMeta_data()) Some(cc.getMeta_data()) else None
              // Use compressed size: if there is no compression codec this will be equal to
              // uncompressed size
              cc_meta.map(_.getTotal_compressed_size).getOrElse(0L)
            }.sum
            val values = Array(
              id, // row group id within file
              size,
              rg.getTotal_byte_size(), // required Thrift field
              rg.getNum_rows(), // required Thrift field
              rg.getColumns().size(), // required Thrift field
              file.path
            )
            MetadataFileFormat.toRow(values)
          }.toIterator
        case ParquetColumnLevel =>
          metadata.getRow_groups().asScala.zipWithIndex.flatMap { case (rg, rgid) =>
            rg.getColumns().asScala.zipWithIndex.map { case (cc, ccid) =>
              val cc_meta = if (cc.isSetMeta_data()) Some(cc.getMeta_data()) else None

              val values = Array(
                rgid, // row group id
                ccid, // column id within row group
                cc.getFile_offset(), // required
                cc_meta.map(_.getType.toString), // required
                cc_meta.map(_.getEncodings.asScala.map(_.toString)), // required
                cc_meta.map(_.getPath_in_schema.asScala.mkString(".")), // required
                cc_meta.map(_.getCodec.toString), // required
                cc_meta.map(_.getNum_values), // required
                cc_meta.map(_.getTotal_uncompressed_size), // required
                cc_meta.map(_.getTotal_compressed_size), // required
                cc_meta.map(_.getData_page_offset), // required
                cc_meta.flatMap { t =>
                  if (t.isSetIndex_page_offset) Some(t.getIndex_page_offset) else None
                }, // optional
                cc_meta.flatMap { t =>
                  if (t.isSetDictionary_page_offset) Some(t.getDictionary_page_offset) else None
                }, // optional
                // Optional Thrift fields
                if (cc.isSetOffset_index_offset()) Some(cc.getOffset_index_offset()) else None,
                if (cc.isSetOffset_index_length()) Some(cc.getOffset_index_length()) else None,
                if (cc.isSetColumn_index_offset()) Some(cc.getColumn_index_offset()) else None,
                if (cc.isSetColumn_index_length()) Some(cc.getColumn_index_length()) else None,
                file.path
              )

              MetadataFileFormat.toRow(values)
            }
          }.toIterator
        case ParquetPageLevel =>
          // TODO: Improve the code below, it may not be very efficient, i.e. sending a lot of
          // requests to S3 or Azure when reading pages due to seeks.

          val path = new Path(file.path)
          val fs = path.getFileSystem(conf)

          val columnInfo = metadata.getRow_groups.asScala.zipWithIndex.flatMap { case (rg, rgid) =>
            rg.getColumns.asScala.zipWithIndex.map { case (cc, ccid) =>
              val cc_meta = if (cc.isSetMeta_data) Some(cc.getMeta_data()) else None
              // Don't rely on col offset, it can be set to data page offset when dict exists
              val cc_offset = cc_meta.map { t =>
                if (t.isSetDictionary_page_offset) {
                  t.getDictionary_page_offset
                } else {
                  t.getData_page_offset
                }
              }.getOrElse(0L)
              // Compressed size is smaller or is equal to uncompressed size
              val cc_size = cc_meta.map(_.getTotal_compressed_size).getOrElse(0L)
              (rgid, ccid, cc_offset, cc_size)
            }
          }

          val iter = new Iterator[Row] {
            val cols = columnInfo
            var col_idx = 0
            var col_size = 0L
            var num_pages = 0
            val in = fs.open(path)

            override def hasNext: Boolean = col_idx < cols.length && col_size < cols(col_idx)._4

            override def next: Row = {
              val offset = cols(col_idx)._3
              in.seek(offset + col_size)

              // Absolute offset to the page header within the file
              val header_offset_within_file = offset + col_size

              val ph = Util.readPageHeader(in)

              val page_header_size = in.getPos() - (offset + col_size)

              // Only one header is set
              val data_ph = if (ph.isSetData_page_header) Some(ph.getData_page_header) else None
              val index_ph = if (ph.isSetIndex_page_header) Some(ph.getIndex_page_header) else None
              val dict_ph =
                if (ph.isSetDictionary_page_header) Some(ph.getDictionary_page_header) else None
              val data_v2_ph =
                if (ph.isSetData_page_header_v2) Some(ph.getData_page_header_v2) else None

              // Not set for index page
              val page_num_values =
                dict_ph.map { t => t.getNum_values.toLong }.orElse {
                data_ph.map { t => t.getNum_values.toLong }.orElse {
                data_v2_ph.map { t => t.getNum_values.toLong }}}

              // Not set for index page
              val page_encoding =
                dict_ph.map { t => t.getEncoding.toString }.orElse {
                data_ph.map { t => t.getEncoding.toString }.orElse {
                data_v2_ph.map { t => t.getEncoding.toString }}}

              // Not set for index and dictionary
              val page_def_encoding =
                data_ph.map { t => t.getDefinition_level_encoding.toString }.orElse {
                data_v2_ph.map { t => Encoding.RLE.toString }} // always encoded as RLE

              // Not set for index and dictionary
              val page_rep_encoding =
                data_ph.map { t => t.getRepetition_level_encoding.toString }.orElse {
                data_v2_ph.map { t => Encoding.RLE.toString }} // always encoded as RLE

              val statistics: Option[Statistics] =
                data_ph.flatMap { t =>
                  if (t.isSetStatistics) Some(t.getStatistics) else None }.orElse {
                data_v2_ph.flatMap { t => if (t.isSetStatistics) Some(t.getStatistics) else None }}

              val values = Array(
                cols(col_idx)._1, // row group id
                cols(col_idx)._2, // column id within row group
                num_pages, // page id within column
                ph.getType.toString, // required Thrift field
                header_offset_within_file,
                page_header_size,
                ph.getUncompressed_page_size.toLong, // required Thrift field
                ph.getCompressed_page_size.toLong, // required Thrift field
                if (ph.isSetCrc()) Some(ph.getCrc()) else None,
                page_num_values,
                page_encoding,
                page_def_encoding,
                page_rep_encoding,
                MetadataFileFormat.getStatistics(statistics),
                file.path
              )

              // Account for page header
              col_size += page_header_size
              // Compressed page size is either smaller or is equal to uncompressed size
              col_size += ph.getCompressed_page_size

              num_pages += 1

              if (col_size >= cols(col_idx)._4) {
                col_idx += 1
                num_pages = 0
                col_size = 0L
              }

              MetadataFileFormat.toRow(values)
            }

            def close(): Unit = {
              in.close()
            }
          }

          val taskContext = Option(TaskContext.get())
          taskContext.foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))

          iter
      }
    }
  }
}
