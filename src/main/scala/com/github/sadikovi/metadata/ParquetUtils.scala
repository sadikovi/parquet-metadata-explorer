package com.github.sadikovi.metadata

import java.util.Arrays

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession

import org.apache.parquet.format._
import org.apache.parquet.schema.MessageType
import org.apache.parquet.format.converter.SchemaUtil

/** Helper functions for Parquet */
object ParquetUtils {
  /**
   * Returns true if provided file contains Parquet MAGIC bytes. Used to infer Parquet source.
   * Should NOT be used in FileFormat classes, use `readParquetMetadata` instead.
   */
  def isParquetFile(spark: SparkSession, path: Path): Boolean = {
    val conf = spark.sessionState.newHadoopConf
    val fs = path.getFileSystem(conf)
    val in = fs.open(path)
    try {
      val tmp = new Array[Byte](4)
      val bytesRead = in.read(tmp, 0, 4)
      bytesRead == 4 && Arrays.equals(tmp, Array[Byte]('P', 'A', 'R', '1'))
    } finally {
      in.close()
    }
  }

  /**
   * Reads Thrift FileMetaData from parquet-format.
   * If size is not provided, fetches file status.
   *
   * Returns FileMetaData object and size of the serialized object on disk.
   */
  def getParquetMetadataAndSize(
      path: Path,
      conf: Configuration,
      size: Option[Long] = None): (FileMetaData, Long) = {
    val fs = path.getFileSystem(conf)
    val fileLen = size match {
      case Some(len) => len
      case None => fs.getFileStatus(path).getLen()
    }
    val in = fs.open(path)
    try {
      val tmp = new Array[Byte](4)
      // Check that it is a valid Parquet file
      in.readFully(tmp)
      if (!Arrays.equals(tmp, Array[Byte]('P', 'A', 'R', '1'))) {
        throw new RuntimeException(s"$path is invalid Parquet file, magic: ${Arrays.toString(tmp)}")
      }
      in.seek(fileLen - 8) // 4 bytes metadata length + 4 bytes magic
      in.readFully(tmp)

      val metadataLen =
        (tmp(0) & 0xff) |
        ((tmp(1) & 0xff) << 8) |
        ((tmp(2) & 0xff) << 16) |
        ((tmp(3) & 0xff) << 24);

      if (fileLen < metadataLen + 8) {
        throw new RuntimeException(
          s"EOF when reading Parquet metadata for $path, file $fileLen, metadata $metadataLen")
      }
      in.seek(fileLen - metadataLen - 8)
      (Util.readFileMetaData(in), metadataLen)
    } finally {
      in.close()
    }
  }

  /**
   * Reads Thrift FileMetaData from parquet-format.
   * If size is not provided, fetches file status.
   */
  def readParquetMetadata(
      path: Path,
      conf: Configuration,
      size: Option[Long] = None): FileMetaData = {
    getParquetMetadataAndSize(path, conf, size)._1
  }
}

////////////////////////////////////////////////////////////////
// Parquet metadata wrappers for Thrift field extraction
////////////////////////////////////////////////////////////////

class FileMetadata(val metadata: FileMetaData) {
  /** Returns Parquet schema */
  val schema: MessageType =
    SchemaUtil.fromParquetSchema(metadata.getSchema, metadata.getColumn_orders) // required field

  /** Returns a number of rows in this file */
  val numRows: Long = metadata.getNum_rows // required field

  /** Returns created-by string in the Parquet footer */
  val createdBy: Option[String] =
    if (metadata.isSetCreated_by) Option(metadata.getCreated_by) else None // optional field

  /** Returns optional key-value metadata stored in the Parquet footer */
  val keyValueMetadata: Option[Map[String, String]] = {
    if (metadata.isSetKey_value_metadata) {
      val map = metadata.getKey_value_metadata.asScala.map { pair =>
        // Each pair is KeyValue struct
        val key = pair.getKey // required field
        // TODO: Option[String] does not seem to work with values of MapType
        val value = if (pair.isSetValue) pair.getValue else null // optional
        (key, value)
      }.toMap
      Option(map)
    } else {
      None
    }
  }

  /** Returns the list of row groups in the file */
  val rowGroups: Seq[RowGroupMetadata] = {
    metadata.getRow_groups.asScala.zipWithIndex.map { case (rowGroup, id) =>
      new RowGroupMetadata(rowGroup, id)
    }
  }
}

class RowGroupMetadata(val rowGroup: RowGroup, val id: Int) {
  /** Returns the list of columns in this row group */
  val columns: Seq[ColumnMetadata] = {
    rowGroup.getColumns().asScala.zipWithIndex.map { case (columnChunk, id) =>
      new ColumnMetadata(columnChunk, id, this.id)
    }
  }

  /**
   * Returns row group offset within the file.
   *
   * Although the list of columns is required, it could be empty theoretically.
   * We will return 0 if the columns do not exist
   */
  val fileOffset: Long = columns.headOption.map(_.fileOffset).getOrElse(0)

  /** Returns total compressed size of all columns in the row group */
  val totalCompressedSize: Long = columns.map(_.totalCompressedSize).sum

  /** Returns total uncompressed size, alternatively called "total_byte_size" */
  val totalUncompressedSize: Long = rowGroup.getTotal_byte_size() // required Thrift field

  /** Returns the number of rows in this row group */
  val numRows: Long = rowGroup.getNum_rows() // required Thrift field

  /** Returns the number of columns in this row group */
  val numColumns: Int = columns.size
}

class ColumnMetadata(val column: ColumnChunk, val id: Int, val rowGroupId: Int) {
  // For some odd reason, column metadata is optional in Parquet
  private val metadata: Option[ColumnMetaData] = {
    if (column.isSetMeta_data()) Some(column.getMeta_data()) else None
  }

  /**
   * Returns an offset within the file for this column.
   *
   * Don't rely on Thrift field for column offset, it can be set to data page offset
   * when dictionary page exists, and sometimes can even be set incorrectly, e.g.
   * "parquet-datasets/datasets1/alltypes_plain.snappy.parquet".
   */
  def fileOffset: Long =
    dictionaryPageOffset
      .orElse(dataPageOffset)
      .getOrElse(column.getFile_offset()) // required Thrift field, use if no other offsets

  /** Returns the type for this column */
  val columnType: Option[String] = metadata.map(_.getType.toString) // required Thrift field

  /** Returns the list of encodings for this column */
  val encodings: Option[Seq[String]] =
    metadata.map(_.getEncodings.asScala.map(_.toString)) // required Thrift field

  /** Returns column path */
  val path: Option[String] =
    metadata.map(_.getPath_in_schema.asScala.mkString(".")) // required Thrift field

  /** Returns compression codec */
  val compressionCodec: Option[String] =
    metadata.map(_.getCodec.toString) // required Thrift field

  /** Returns the number of values in this column */
  val numValues: Long = metadata.map(_.getNum_values).getOrElse(0) // required Thrift field

  /** Returns total compressed size in bytes */
  val totalCompressedSize: Long =
    metadata.map(_.getTotal_compressed_size).getOrElse(0) // required Thrift field

  /** Returns total uncompressed size in bytes */
  val totalUncompressedSize: Long =
    metadata.map(_.getTotal_uncompressed_size).getOrElse(0) // required Thrift field

  /** Returns offset to the first data page in the column */
  val dataPageOffset: Option[Long] = metadata.map(_.getData_page_offset) // required Thrift field

  /**
   * Returns offset to the dictionary page in the column if exists.
   *
   * Parquet libraries do not set the dictionary offset and instead set the data page offset with
   * dictionary offset resulting in null when there is a dictionary page available.
   *
   * TODO: This can be solved by checking if a dictionary page exists, currently it is not
   * implemented.
   */
  val dictionaryPageOffset: Option[Long] = metadata.flatMap { meta =>
    if (meta.isSetDictionary_page_offset) Some(meta.getDictionary_page_offset) else None
  }

  /** Returns offset to the index page in the column if exists */
  val indexPageOffset: Option[Long] = metadata.flatMap { meta =>
    if (meta.isSetIndex_page_offset) Some(meta.getIndex_page_offset) else None
  }

  /** Returns the offset of OffsetIndex if exists */
  val offsetIndexOffset: Option[Long] =
    if (column.isSetOffset_index_offset()) Some(column.getOffset_index_offset()) else None

  /** Returns the size of OffsetIndex if exists */
  val offsetIndexSize: Option[Int] =
    if (column.isSetOffset_index_length()) Some(column.getOffset_index_length()) else None

  /** Returns the offset of ColumnIndex if exists */
  val columnIndexOffset: Option[Long] =
    if (column.isSetColumn_index_offset()) Some(column.getColumn_index_offset()) else None

  /** Returns the size of ColumnIndex if exists */
  val columnIndexSize: Option[Int] =
    if (column.isSetColumn_index_length()) Some(column.getColumn_index_length()) else None
}

class PageMetadata(
    val pageHeader: PageHeader,
    val id: Int,
    val fileOffset: Long, // header offset within the file
    val headerSize: Int) {
  // Specific page headers (only one is set)
  private val dataPageHeader =
    if (pageHeader.isSetData_page_header) Some(pageHeader.getData_page_header) else None
  private val dictPageHeader =
    if (pageHeader.isSetDictionary_page_header) Some(pageHeader.getDictionary_page_header) else None
  private val indexPageHeader =
    if (pageHeader.isSetIndex_page_header) Some(pageHeader.getIndex_page_header) else None
  private val dataV2PageHeader =
    if (pageHeader.isSetData_page_header_v2) Some(pageHeader.getData_page_header_v2) else None

  val pageType: String = pageHeader.getType.toString // required Thrift field

  /** Uncompressed page size in bytes (not including this header) */
  val uncompressedPageSize: Int = pageHeader.getUncompressed_page_size // required Thrift field

  /** Compressed (and potentially encrypted) page size in bytes, not including this header */
  val compressedPageSize: Int = pageHeader.getCompressed_page_size // required Thrift field

  /** CRC value */
  val crc: Option[Int] = if (pageHeader.isSetCrc()) Some(pageHeader.getCrc()) else None

  /** Number of values (not set for index pages) */
  val numValues: Option[Int] =
    dictPageHeader.map(_.getNum_values).orElse {
    dataPageHeader.map(_.getNum_values).orElse {
    dataV2PageHeader.map(_.getNum_values)}}

  /** Page encoding (not set for index page) */
  val encoding: Option[String] =
    dictPageHeader.map(_.getEncoding.toString).orElse {
    dataPageHeader.map(_.getEncoding.toString).orElse {
    dataV2PageHeader.map(_.getEncoding.toString)}}

  /** Page definition encoding (not set for index and dictionary) */
  val definitionEncoding: Option[String] =
    dataPageHeader.map(_.getDefinition_level_encoding.toString).orElse {
    dataV2PageHeader.map(_ => Encoding.RLE.toString)} // always encoded as RLE

  /** Page repetition encoding (not set for index and dictionary) */
  val repetitionEncoding: Option[String] =
    dataPageHeader.map(_.getRepetition_level_encoding.toString).orElse {
    dataV2PageHeader.map(_ => Encoding.RLE.toString)} // always encoded as RLE

  /** Page statistics */
  val statistics: Option[PageStatistics] = {
    val rawStats = dataPageHeader.flatMap { header =>
      if (header.isSetStatistics) Some(header.getStatistics) else None
    }.orElse {
      dataV2PageHeader.flatMap { header =>
        if (header.isSetStatistics) Some(header.getStatistics) else None }
    }
    rawStats.map(new PageStatistics(_))
  }
}

class PageStatistics(stats: Statistics) {
  /** Number of nulls in the page */
  val nullCount: Option[Long] =
    if (stats.isSetNull_count) Some(stats.getNull_count) else None

  /** Number of distinct values in the page */
  val distinctCount: Option[Long] =
    if (stats.isSetDistinct_count) Some(stats.getDistinct_count) else None

  /**
   * Length of the min value.
   * We report the length for privacy reasons.
   */
  val minValueLength: Option[Int] = {
    val min_value = if (stats.isSetMin_value) Some(stats.getMin_value) else None
    val min = if (stats.isSetMin) Some(stats.getMin) else None
    min_value.orElse(min).map(_.length)
  }

  /**
   * Length of the max value.
   * We report the length for privacy reasons.
   */
  val maxValueLength: Option[Int] = {
    val maxValue = if (stats.isSetMax_value) Some(stats.getMax_value) else None
    val max = if (stats.isSetMax) Some(stats.getMax) else None
    maxValue.orElse(max).map(_.length)
  }
}
