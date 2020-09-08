package com.github.sadikovi.metadata

import java.util.Arrays

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession

import org.apache.parquet.format.{FileMetaData, Util}

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
   */
  def readParquetMetadata(
      path: Path,
      conf: Configuration,
      size: Option[Long] = None): FileMetaData = {
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
      Util.readFileMetaData(in)
    } finally {
      in.close()
    }
  }
}
