package com.github.sadikovi.metadata

import org.apache.spark.sql.types._

/**
 * Different levels of metadata granularity.
 */
abstract class MetadataLevel extends Serializable {
  /** Returns schema for the level */
  def schema: StructType

  override def toString: String = this.getClass.getSimpleName.stripSuffix("$")
}

object FileLevel extends MetadataLevel {
  override def schema: StructType = StructType(
    StructField("filepath", StringType) ::
    StructField("filename", StringType) ::
    StructField("size", LongType) ::
    StructField("mtime", LongType) ::
    StructField("partition", MapType(StringType, StringType)) ::
    Nil)
}

object ParquetFileLevel extends MetadataLevel {
  override def schema: StructType =
    FileLevel.schema
      .add(StructField("metadata_size", LongType))
      .add(StructField("schema", StringType))
      .add(StructField("num_rows", LongType))
      .add(StructField("num_row_groups", IntegerType))
      .add(StructField("created_by", StringType))
      .add(StructField("key_value_metadata", MapType(StringType, StringType)))
}

object ParquetRowGroupLevel extends MetadataLevel {
  override def schema: StructType = StructType(
    StructField("row_group_id", IntegerType) ::
    StructField("file_offset", LongType) ::
    StructField("total_compressed_size", LongType) ::
    StructField("total_uncompressed_size", LongType) ::
    StructField("num_rows", LongType) ::
    StructField("num_columns", IntegerType) ::
    StructField("filepath", StringType) ::
    Nil)
}

object ParquetColumnLevel extends MetadataLevel {
  override def schema: StructType = StructType(
    StructField("row_group_id", IntegerType) ::
    StructField("column_id", IntegerType) ::
    StructField("file_offset", LongType) ::
    StructField("total_compressed_size", LongType) ::
    StructField("total_uncompressed_size", LongType) ::
    StructField("path", StringType) ::
    StructField("type", StringType) ::
    StructField("encodings", ArrayType(StringType)) ::
    StructField("compression", StringType) ::
    StructField("num_values", LongType) ::
    StructField("statistics", StructType(
      StructField("null_count", LongType) ::
      StructField("distinct_count", LongType) ::
      StructField("min", BinaryType) ::
      StructField("max", BinaryType) ::
      StructField("min_value", BinaryType) ::
      StructField("max_value", BinaryType) ::
      Nil)) ::
    StructField("data_page_offset", LongType) ::
    StructField("dictionary_page_offset", LongType) ::
    StructField("index_page_offset", LongType) ::
    StructField("offset_index_offset", LongType) ::
    StructField("offset_index_length", IntegerType) ::
    StructField("column_index_offset", LongType) ::
    StructField("column_index_length", IntegerType) ::
    StructField("filepath", StringType) ::
    Nil)
}

object ParquetPageLevel extends MetadataLevel {
  override def schema: StructType = StructType(
    StructField("row_group_id", IntegerType) ::
    StructField("column_id", IntegerType) ::
    StructField("page_id", IntegerType) ::
    StructField("page_type", StringType) ::
    StructField("page_header_offset", LongType) ::
    StructField("page_header_size", IntegerType) ::
    StructField("page_compressed_size", IntegerType) ::
    StructField("page_uncompressed_size", IntegerType) ::
    StructField("crc", IntegerType) ::
    StructField("num_values", IntegerType) ::
    StructField("encoding", StringType) ::
    StructField("definition_level_encoding", StringType) ::
    StructField("repetition_level_encoding", StringType) ::
    StructField("statistics", StructType(
      StructField("null_count", LongType) ::
      StructField("distinct_count", LongType) ::
      StructField("min", BinaryType) ::
      StructField("max", BinaryType) ::
      StructField("min_value", BinaryType) ::
      StructField("max_value", BinaryType) ::
      Nil)) ::
    StructField("page_content", ArrayType(ByteType)) ::
    StructField("filepath", StringType) ::
    Nil)
}
