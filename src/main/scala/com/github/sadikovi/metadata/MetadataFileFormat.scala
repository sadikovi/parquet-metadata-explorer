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
      // Must conform to FileLevel schema
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
    maxPartitions: Int,
    bufferSize: Int)
  extends MetadataFileFormat(spark, fileIndex, level, maxPartitions) {

  require(
    level == ParquetFileLevel ||
    level == ParquetRowGroupLevel ||
    level == ParquetColumnLevel ||
    level == ParquetPageLevel,
    s"Unsupported $level for $this")

  require(bufferSize > 0, s"Unsupported buffer size value $bufferSize")

  override def buildScan(rdd: RDD[FileInfo], partitionSchema: StructType): RDD[Row] = {
    val broadcastedHadoopConf =
      spark.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    rdd.flatMap { file =>
      val conf = broadcastedHadoopConf.value.value
      val metadata = new FileMetadata(
        ParquetUtils.readParquetMetadata(new Path(file.path), conf, Some(file.size)))

      level match {
        case ParquetFileLevel =>
          val partMap = MetadataFileFormat.getPartitionMap(partitionSchema, file.partitionValues)
          // Must conform to ParquetFileLevel schema
          val values = Array(
            file.path,
            file.name,
            file.size,
            file.mtime,
            partMap,
            metadata.schema.toString,
            metadata.numRows,
            metadata.createdBy,
            metadata.keyValueMetadata
          )
          Iterator(MetadataFileFormat.toRow(values))
        case ParquetRowGroupLevel =>
          metadata.rowGroups.map { rowGroup =>
            // Must conform to ParquetRowGroupLevel schema
            val values = Array(
              rowGroup.id,
              rowGroup.fileOffset,
              rowGroup.totalCompressedSize,
              rowGroup.totalUncompressedSize,
              rowGroup.numRows,
              rowGroup.numColumns,
              file.path
            )
            MetadataFileFormat.toRow(values)
          }.toIterator
        case ParquetColumnLevel =>
          metadata.rowGroups.flatMap { rowGroup =>
            rowGroup.columns.map { column =>
              // Must conform to ParquetColumnLevel schema
              val values = Array(
                column.rowGroupId,
                column.id,
                column.fileOffset,
                column.totalCompressedSize,
                column.totalUncompressedSize,
                column.path,
                column.columnType,
                column.encodings,
                column.compressionCodec,
                column.numValues,
                column.dataPageOffset,
                column.dictionaryPageOffset,
                column.indexPageOffset,
                column.offsetIndexOffset,
                column.offsetIndexSize,
                column.columnIndexOffset,
                column.columnIndexSize,
                file.path
              )

              MetadataFileFormat.toRow(values)
            }
          }.toIterator
        case ParquetPageLevel =>
          val path = new Path(file.path)
          val fs = path.getFileSystem(conf)

          val columnInfo = metadata.metadata.getRow_groups.asScala.zipWithIndex.flatMap { case (rg, rgid) =>
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
            val cols = metadata.rowGroups.flatMap(_.columns)
            var colIndex = 0
            var colSize = 0L
            var pageId = 0
            val in = new RemoteInputStream(fs.open(path), bufferSize)

            override def hasNext: Boolean = {
              colIndex < cols.length && colSize < cols(colIndex).totalCompressedSize
            }

            override def next: Row = {
              val offset = cols(colIndex).fileOffset
              in.seek(offset + colSize)

              // Absolute offset to the page header within the file
              val fileOffset = offset + colSize // header offset within the file
              val pageHeader = Util.readPageHeader(in)
              val pageHeaderSize = (in.getPos() - (offset + colSize)).toInt // must fit within int
              val page = new PageMetadata(pageHeader, pageId, fileOffset, pageHeaderSize)

              val values = Array(
                cols(colIndex).rowGroupId,
                cols(colIndex).id,
                page.id,
                page.pageType,
                page.fileOffset,
                page.headerSize,
                page.compressedPageSize,
                page.uncompressedPageSize,
                page.crc,
                page.numValues,
                page.encoding,
                page.definitionEncoding,
                page.repetitionEncoding,
                page.statistics,
                file.path
              )

              // Account for page header
              colSize += pageHeaderSize
              // Compressed page size is either smaller or is equal to uncompressed size
              colSize += page.compressedPageSize
              // Increment page id for the next page
              pageId += 1

              if (colSize >= cols(colIndex).totalCompressedSize) {
                // Finished the column, reset the counters
                colIndex += 1
                colSize = 0L
                pageId = 0
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
