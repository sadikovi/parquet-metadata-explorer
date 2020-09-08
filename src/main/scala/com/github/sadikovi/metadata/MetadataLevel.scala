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
    StructField("path", StringType) ::
    StructField("name", StringType) ::
    StructField("size", LongType) ::
    StructField("mtime", LongType) ::
    StructField("partition", MapType(StringType, StringType)) ::
    Nil)
}

object ParquetFileLevel extends MetadataLevel {
  override def schema: StructType =
    FileLevel.schema
      .add(StructField("schema", StringType))
      .add(StructField("num_rows", LongType))
      .add(StructField("created_by", StringType))
      .add(StructField("key_value_metadata", ArrayType(StructType(
        StructField("key", StringType) ::
        StructField("value", StringType) ::
        Nil))))
}

object ParquetRowGroupLevel extends MetadataLevel {
  override def schema: StructType = StructType(
    StructField("row_group_id", IntegerType) ::
    StructField("size", LongType) ::
    StructField("total_byte_size", LongType) ::
    StructField("num_rows", LongType) ::
    StructField("num_columns", IntegerType) ::
    StructField("path", StringType) ::
    Nil)
}

object ParquetColumnLevel extends MetadataLevel {
  override def schema: StructType = StructType(
    StructField("row_group_id", IntegerType) ::
    StructField("column_id", IntegerType) ::
    StructField("file_offset", LongType) ::
    StructField("type", StringType) ::
    StructField("encodings", ArrayType(StringType)) ::
    StructField("path_in_schema", StringType) ::
    StructField("codec", StringType) ::
    StructField("num_values", LongType) ::
    StructField("total_uncompressed_size", LongType) ::
    StructField("total_compressed_size", LongType) ::
    StructField("data_page_offset", LongType) ::
    StructField("index_page_offset", LongType) ::
    StructField("dictionary_page_offset", LongType) ::
    StructField("offset_index_offset", LongType) ::
    StructField("offset_index_length", LongType) ::
    StructField("column_index_offset", LongType) ::
    StructField("column_index_length", LongType) ::
    StructField("path", StringType) ::
    Nil)
}

object ParquetPageLevel extends MetadataLevel {
  override def schema: StructType = StructType(
    StructField("row_group_id", IntegerType) ::
    StructField("column_id", IntegerType) ::
    StructField("page_id", IntegerType) ::
    StructField("page_type", StringType) ::
    StructField("page_header_offset_within_file", LongType) ::
    StructField("page_header_size", LongType) ::
    StructField("uncompressed_page_size", LongType) ::
    StructField("compressed_page_size", LongType) ::
    StructField("crc", IntegerType) ::
    StructField("num_values", LongType) ::
    StructField("encoding", StringType) ::
    StructField("definition_level_encoding", StringType) ::
    StructField("repetition_level_encoding", StringType) ::
    StructField("statistics", StructType(
      StructField("null_count", LongType) ::
      StructField("distinct_count", LongType) ::
      StructField("min_size", LongType) ::
      StructField("max_size", LongType) ::
      Nil)) ::
    StructField("path", StringType) ::
    Nil)
}
