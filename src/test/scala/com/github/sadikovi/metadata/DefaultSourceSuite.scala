package com.github.sadikovi.metadata

import org.apache.hadoop.fs.Path

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class DefaultSourceSuite extends UnitTestSuite with SparkLocal {
  override def beforeAll {
    startSparkSession()
  }

  override def afterAll {
    stopSparkSession()
  }

  def writeDF(): DataFrame = {
    spark.range(0, 1000, 1, 4)
      .withColumn("col1", lit(1))
      .withColumn("col2", lit(2))
  }

  def readDF: DataFrameReader = {
    spark.read.format("metadata")
  }

  test("SerializableFileStatus, isStatus") {
    class MyFileStatus { }
    class SerializedFileStatus { }
    class MyStatus { }
    assert(SerializableFileStatus.isStatus(new MyFileStatus()))
    assert(SerializableFileStatus.isStatus(new SerializedFileStatus()))
    assert(!SerializableFileStatus.isStatus(new MyStatus()))
  }

  test("SerializedFileStatus, call methods") {
    class MyFileStatus {
      def getLen: Long = 1
      def getModificationTime: Long = 2
      def getPath: Path = new Path("/test")
    }

    val status = new SerializableFileStatus(new MyFileStatus())
    assert(status.getPath === new Path("/test"))
    assert(status.getLen === 1)
    assert(status.getModificationTime === 2)
  }

  test("DefaultSource, invalid source") {
    withTempDir { dir =>
      val err = intercept[IllegalArgumentException] {
        val df = readDF.option("source", "invalid").load(dir.toString)
      }
      assert(err.getMessage.contains("Invalid source:"))
    }
  }

  test("DefaultSource, invalid level") {
    withTempDir { dir =>
      val err = intercept[IllegalArgumentException] {
        val df = readDF.option("level", "invalid").load(dir.toString)
      }
      assert(err.getMessage.contains("Invalid level:"))
    }
  }

  test("DefaultSource, incompatible source and level") {
    withTempDir { dir =>
      val err = intercept[IllegalArgumentException] {
        val df = readDF
          .option("source", "file")
          .option("level", "rowgroup")
          .load(dir.toString)
      }
      assert(err.getMessage.contains("Source 'file' does not support 'rowgroup' level"))
    }
  }

  test("test file level for JSON") {
    withTempDir { dir =>
      writeDF.write.partitionBy("col1", "col2").json(dir + "/range")
      val df = readDF.option("level", "file").load(dir + "/range")
      assert(df.count === 4)
      assert(df.schema === FileLevel.schema)
      df.show
    }
  }

  test("test file level") {
    withTempDir { dir =>
      writeDF.write.partitionBy("col1", "col2").parquet(dir + "/range")
      val df = readDF.option("level", "file").load(dir + "/range")
      assert(df.count === 4)
      assert(df.schema === ParquetFileLevel.schema)
      df.show
    }
  }

  test("test rowgroup level") {
    withTempDir { dir =>
      writeDF.write.partitionBy("col1", "col2").parquet(dir + "/range")
      val df = readDF.option("level", "rowgroup").load(dir + "/range")
      assert(df.count === 4)
      assert(df.schema === ParquetRowGroupLevel.schema)
      df.show
    }
  }

  test("test column level") {
    withTempDir { dir =>
      writeDF.write.partitionBy("col1").parquet(dir + "/range")
      val df = readDF.option("level", "column").load(dir + "/range")
      assert(df.count === 4 * 2)
      assert(df.schema === ParquetColumnLevel.schema)
      df.show
    }
  }

  test("test page level") {
    withTempDir { dir =>
      writeDF.write.partitionBy("col1").parquet(dir + "/range")
      val df = readDF.option("level", "page").load(dir + "/range")
      assert(df.count === 12)
      assert(df.schema === ParquetPageLevel.schema)
      df.show
    }
  }

  test("test projection") {
    withTempDir { dir =>
      writeDF.write.partitionBy("col1").parquet(dir + "/range")
      val df = readDF.option("level", "file").load(dir + "/range")
      val proj1 = df.select("mtime", "name", "size", "path").collect.toSeq
      val proj2 = df.cache.select("mtime", "name", "size", "path")
      checkAnswer(proj2, proj1)
    }
  }

  test("test filter") {
    withTempDir { dir =>
      writeDF.write.partitionBy("col1").parquet(dir + "/range")
      val df = readDF.option("level", "file").load(dir + "/range")
      assert(df.filter("size < 0").count == 0)
    }
  }

  test("read single file") {
    withTempDir { dir =>
      writeDF.coalesce(1).write.parquet(dir + "/range")
      val file = fs.listStatus(new Path(dir, "range"))
        .map(_.getPath.toString)
        .filter(_.endsWith(".parquet"))
        .head
      val df = readDF.option("level", "file").load(file)
      assert(df.count === 1)
      assert(df.schema === ParquetFileLevel.schema)
      df.show
    }
  }
}
