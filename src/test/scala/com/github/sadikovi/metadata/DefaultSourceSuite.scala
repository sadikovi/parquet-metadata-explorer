package com.github.sadikovi.metadata

import java.sql.{Date, Timestamp}

import org.apache.hadoop.fs.Path

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

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

  test("MetadataFileFormat, getPartitionMap") {
    // Spark SQL ensures that the schema always matches the partition values
    val schema = StructType(
      StructField("boolean", BooleanType) ::
      StructField("int", IntegerType) ::
      StructField("long", LongType) ::
      StructField("double", DoubleType) ::
      StructField("string", StringType) ::
      StructField("date", DateType) ::
      StructField("timestamp", TimestampType) ::
      Nil)

    val parts = InternalRow(
      true,
      1,
      2L,
      3.3,
      "abc",
      Date.valueOf("2020-01-01"),
      Timestamp.valueOf("2020-02-02 01:02:03")
    )

    val res = MetadataFileFormat.getPartitionMap(schema, parts)
    val exp = Map(
      "boolean" -> "true",
      "int" -> "1",
      "long" -> "2",
      "double" -> "3.3",
      "string" -> "abc",
      "date" -> "2020-01-01",
      "timestamp" -> "2020-02-02 01:02:03.0")

    assert(res === exp)
  }

  test("test file level for JSON") {
    withTempDir { dir =>
      writeDF.write.partitionBy("col1", "col2").json(dir + "/range")
      val df = readDF.option("level", "file").load(dir + "/range")
      assert(df.count === 4)
      assert(df.schema === FileLevel.schema)
    }
  }

  test("test file level") {
    val implicits = spark.implicits
    import implicits._

    withTempDir { dir =>
      writeDF.write.partitionBy("col1", "col2").parquet(dir + "/range")
      val df = readDF.option("level", "file").load(dir + "/range").cache

      assert(df.schema === ParquetFileLevel.schema)
      assert(df.count === 4)

      checkAnswer(
        df.select("size", "partition", "num_rows", "num_row_groups", "metadata_size"),
        Seq(
          Row(1464L, Map("col1" -> "1", "col2" -> "2"), 250L, 1, 377L),
          Row(1467L, Map("col1" -> "1", "col2" -> "2"), 250L, 1, 377L),
          Row(1470L, Map("col1" -> "1", "col2" -> "2"), 250L, 1, 377L),
          Row(1468L, Map("col1" -> "1", "col2" -> "2"), 250L, 1, 377L)
        )
      )

      val currTime = System.currentTimeMillis
      df.select("mtime").as[Long].collect.foreach { mtime =>
        assert(mtime >= currTime - 60 * 1000 && mtime <= currTime + 60 * 1000)
      }

      df.select("schema").as[String].collect.foreach { schema =>
        assert(schema.startsWith("message spark_schema"))
      }

      df.select("created_by").as[String].collect.foreach { schema =>
        assert(schema.startsWith("parquet-mr"))
      }

      assert(df.select("key_value_metadata").as[Map[String, String]]
        .collect.flatMap(_.keys).toSet ===
          Set("org.apache.spark.version", "org.apache.spark.sql.parquet.row.metadata"))

      df.select("filepath", "filename").as[(String, String)].collect.foreach { case (path, name) =>
        assert(name.length > 0)
        assert(path.endsWith(name))
      }
    }
  }

  test("test rowgroup level") {
    val implicits = spark.implicits
    import implicits._

    withTempDir { dir =>
      writeDF.write.partitionBy("col1", "col2").parquet(dir + "/range")
      val df = readDF.option("level", "rowgroup").load(dir + "/range")

      assert(df.schema === ParquetRowGroupLevel.schema)

      checkAnswer(
        df.drop("filepath"),
        Seq(
          Row(0, 4L, 1075L, 2064L, 250L, 1),
          Row(0, 4L, 1078L, 2064L, 250L, 1),
          Row(0, 4L, 1081L, 2064L, 250L, 1),
          Row(0, 4L, 1079L, 2064L, 250L, 1)
        )
      )

      df.select("filepath").as[String].collect.foreach { path =>
        assert(path.length > 0)
      }
    }
  }

  test("test column level") {
    val implicits = spark.implicits
    import implicits._

    withTempDir { dir =>
      writeDF.write.partitionBy("col1").parquet(dir + "/range")
      val df = readDF.option("level", "column").load(dir + "/range")

      assert(df.schema === ParquetColumnLevel.schema)

      // scalastyle:off
      checkAnswer(
        df.drop("filepath"),
        Seq(
          Row(0, 0, 4L, 1075L, 2064L, "id", "INT64", Seq("BIT_PACKED", "PLAIN"), "SNAPPY", 250L,
            Row(0L, null, Array[Byte](0, 0, 0, 0, 0, 0, 0, 0), Array[Byte](-7, 0, 0, 0, 0, 0, 0, 0), Array[Byte](0, 0, 0, 0, 0, 0, 0, 0), Array[Byte](-7, 0, 0, 0, 0, 0, 0, 0)),
            4L, null, null, null, null, null, null),
          Row(0, 0, 4L, 1078L, 2064L, "id", "INT64", Seq("BIT_PACKED", "PLAIN"), "SNAPPY", 250L,
            Row(0L, null, Array[Byte](-6, 0, 0, 0, 0, 0, 0, 0), Array[Byte](-13, 1, 0, 0, 0, 0, 0, 0), Array[Byte](-6, 0, 0, 0, 0, 0, 0, 0), Array[Byte](-13, 1, 0, 0, 0, 0, 0, 0)),
            4L, null, null, null, null, null, null),
          Row(0, 0, 4L, 1081L, 2064L, "id", "INT64", Seq("BIT_PACKED", "PLAIN"), "SNAPPY", 250L,
            Row(0L, null, Array[Byte](-18, 2, 0, 0, 0, 0, 0, 0), Array[Byte](-25, 3, 0, 0, 0, 0, 0, 0), Array[Byte](-18, 2, 0, 0, 0, 0, 0, 0), Array[Byte](-25, 3, 0, 0, 0, 0, 0, 0)),
            4L, null, null, null, null, null, null),
          Row(0, 0, 4L, 1079L, 2064L, "id", "INT64", Seq("BIT_PACKED", "PLAIN"), "SNAPPY", 250L,
            Row(0L, null, Array[Byte](-12, 1, 0, 0, 0, 0, 0, 0), Array[Byte](-19, 2, 0, 0, 0, 0, 0, 0), Array[Byte](-12, 1, 0, 0, 0, 0, 0, 0), Array[Byte](-19, 2, 0, 0, 0, 0, 0, 0)),
            4L, null, null, null, null, null, null),
          Row(0, 1, 1079L, 70L, 66L, "col2", "INT32", Seq("BIT_PACKED", "PLAIN_DICTIONARY"), "SNAPPY", 250L,
            Row(0L, null, Array[Byte](2, 0, 0, 0), Array[Byte](2, 0, 0, 0), Array[Byte](2, 0, 0, 0), Array[Byte](2, 0, 0, 0)),
            1079L, null, null, null, null, null, null),
          Row(0, 1, 1082L, 70L, 66L, "col2", "INT32", Seq("BIT_PACKED", "PLAIN_DICTIONARY"), "SNAPPY", 250L,
            Row(0L, null, Array[Byte](2, 0, 0, 0), Array[Byte](2, 0, 0, 0), Array[Byte](2, 0, 0, 0), Array[Byte](2, 0, 0, 0)),
            1082L, null, null, null, null, null, null),
          Row(0, 1, 1083L, 70L, 66L, "col2", "INT32", Seq("BIT_PACKED", "PLAIN_DICTIONARY"), "SNAPPY", 250L,
            Row(0L, null, Array[Byte](2, 0, 0, 0), Array[Byte](2, 0, 0, 0), Array[Byte](2, 0, 0, 0), Array[Byte](2, 0, 0, 0)),
            1083L, null, null, null, null, null, null),
          Row(0, 1, 1085L, 70L, 66L, "col2", "INT32", Seq("BIT_PACKED", "PLAIN_DICTIONARY"), "SNAPPY", 250L,
            Row(0L, null, Array[Byte](2, 0, 0, 0), Array[Byte](2, 0, 0, 0), Array[Byte](2, 0, 0, 0), Array[Byte](2, 0, 0, 0)),
            1085L, null, null, null, null, null, null),
        )
      )
      // scalastyle:on

      df.select("filepath").as[String].collect.foreach { path =>
        assert(path.length > 0)
      }
    }
  }

  test("test page level") {
    val implicits = spark.implicits
    import implicits._

    withTempDir { dir =>
      writeDF.write.partitionBy("col1").parquet(dir + "/range")
      val df = readDF.option("level", "page").load(dir + "/range")

      assert(df.schema === ParquetPageLevel.schema)

      // scalastyle:off
      checkAnswer(
        df.drop("filepath"),
        Seq(
          Row(0, 0, 0, "DATA_PAGE", 4L, 64, 1011, 2000, null, 250, "PLAIN", "BIT_PACKED", "BIT_PACKED",
            Row(0L, null, Array[Byte](0, 0, 0, 0, 0, 0, 0, 0), Array[Byte](-7, 0, 0, 0, 0, 0, 0, 0), Array[Byte](0, 0, 0, 0, 0, 0, 0, 0), Array[Byte](-7, 0, 0, 0, 0, 0, 0, 0)), null),
          Row(0, 1, 0, "DICTIONARY_PAGE", 1079L, 13, 6, 4, null, 1, "PLAIN_DICTIONARY", null, null, null, null),
          Row(0, 1, 1, "DATA_PAGE", 1098L, 46, 5, 3, null, 250, "PLAIN_DICTIONARY", "BIT_PACKED", "BIT_PACKED",
            Row(0L, null, Array[Byte](2, 0, 0, 0), Array[Byte](2, 0, 0, 0), Array[Byte](2, 0, 0, 0), Array[Byte](2, 0, 0, 0)), null),
          Row(0, 0, 0, "DATA_PAGE", 4L, 64, 1014, 2000, null, 250, "PLAIN", "BIT_PACKED", "BIT_PACKED",
            Row(0L, null, Array[Byte](-6, 0, 0, 0, 0, 0, 0, 0), Array[Byte](-13, 1, 0, 0, 0, 0, 0, 0), Array[Byte](-6, 0, 0, 0, 0, 0, 0, 0), Array[Byte](-13, 1, 0, 0, 0, 0, 0, 0)), null),
          Row(0, 1, 0, "DICTIONARY_PAGE", 1082L, 13, 6, 4, null, 1, "PLAIN_DICTIONARY", null, null, null, null),
          Row(0, 1, 1, "DATA_PAGE", 1101L, 46, 5, 3, null, 250, "PLAIN_DICTIONARY", "BIT_PACKED", "BIT_PACKED",
            Row(0L, null, Array[Byte](2, 0, 0, 0), Array[Byte](2, 0, 0, 0), Array[Byte](2, 0, 0, 0), Array[Byte](2, 0, 0, 0)), null),
          Row(0, 0, 0, "DATA_PAGE", 4L, 64, 1017, 2000, null, 250, "PLAIN", "BIT_PACKED", "BIT_PACKED",
            Row(0L, null, Array[Byte](-18, 2, 0, 0, 0, 0, 0, 0), Array[Byte](-25, 3, 0, 0, 0, 0, 0, 0), Array[Byte](-18, 2, 0, 0, 0, 0, 0, 0), Array[Byte](-25, 3, 0, 0, 0, 0, 0, 0)), null),
          Row(0, 1, 0, "DICTIONARY_PAGE", 1085L, 13, 6, 4, null, 1, "PLAIN_DICTIONARY", null, null, null, null),
          Row(0, 1, 1, "DATA_PAGE", 1104L, 46, 5, 3, null, 250, "PLAIN_DICTIONARY", "BIT_PACKED", "BIT_PACKED",
            Row(0L, null, Array[Byte](2, 0, 0, 0), Array[Byte](2, 0, 0, 0), Array[Byte](2, 0, 0, 0), Array[Byte](2, 0, 0, 0)), null),
          Row(0, 0, 0, "DATA_PAGE", 4L, 64, 1015, 2000, null, 250, "PLAIN", "BIT_PACKED", "BIT_PACKED",
            Row(0L, null, Array[Byte](-12, 1, 0, 0, 0, 0, 0, 0), Array[Byte](-19, 2, 0, 0, 0, 0, 0, 0), Array[Byte](-12, 1, 0, 0, 0, 0, 0, 0), Array[Byte](-19, 2, 0, 0, 0, 0, 0, 0)), null),
          Row(0, 1, 0, "DICTIONARY_PAGE", 1083L, 13, 6, 4, null, 1, "PLAIN_DICTIONARY", null, null, null, null),
          Row(0, 1, 1, "DATA_PAGE", 1102L, 46, 5, 3, null, 250, "PLAIN_DICTIONARY", "BIT_PACKED", "BIT_PACKED",
            Row(0L, null, Array[Byte](2, 0, 0, 0), Array[Byte](2, 0, 0, 0), Array[Byte](2, 0, 0, 0), Array[Byte](2, 0, 0, 0)), null)
        )
      )
      // scalastyle:on

      df.select("filepath").as[String].collect.foreach { path =>
        assert(path.length > 0)
      }
    }
  }

  test("test page level with page content") {
    val implicits = spark.implicits
    import implicits._

    withTempDir { dir =>
      Seq((1, "a", true), (2, "b", false)).toDF.coalesce(1).write
        .option("compression", "none").parquet(dir + "/range")
      val df = readDF.option("level", "page").option("pagecontent", "true").load(dir + "/range")

      assert(df.schema === ParquetPageLevel.schema)

      // scalastyle:off
      checkAnswer(
        df.drop("filepath"),
        Seq(
          Row(0, 0, 0, "DATA_PAGE", 4L, 45, 8, 8, null, 2, "PLAIN", "BIT_PACKED", "BIT_PACKED",
            Row(0L, null, Array[Byte](1, 0, 0, 0), Array[Byte](2, 0, 0, 0), Array[Byte](1, 0, 0, 0), Array[Byte](2, 0, 0, 0)),
            Array[Byte](1, 0, 0, 0, 2, 0, 0, 0)),
          Row(0, 1, 0, "DATA_PAGE", 57L, 27, 16, 16, null, 2, "PLAIN", "RLE", "BIT_PACKED",
            Row(0L, null, null, null, Array[Byte](97), Array[Byte](98)),
            Array[Byte](2, 0, 0, 0, 3, 3, 1, 0, 0, 0, 97, 1, 0, 0, 0, 98)),
          Row(0, 2, 0, "DATA_PAGE", 100L, 33, 1, 1, null, 2, "PLAIN", "BIT_PACKED", "BIT_PACKED",
            Row(0L, null, Array[Byte](0), Array[Byte](1), Array[Byte](0), Array[Byte](1)),
            Array[Byte](1))
        )
      )
      // scalastyle:on

      df.select("filepath").as[String].collect.foreach { path =>
        assert(path.length > 0)
      }
    }
  }

  test("test projection") {
    withTempDir { dir =>
      writeDF.write.partitionBy("col1").parquet(dir + "/range")
      val df = readDF.option("level", "file").load(dir + "/range")
      val proj1 = df.select("mtime", "filename", "size", "filepath").collect.toSeq
      val proj2 = df.cache.select("mtime", "filename", "size", "filepath")
      checkAnswer(proj2, proj1)
    }
  }

  test("test filter") {
    withTempDir { dir =>
      writeDF.write.partitionBy("col1").parquet(dir + "/range")
      val df = readDF.option("level", "file").load(dir + "/range")
      assert(df.filter("size < 0").count === 0)
      assert(df.filter("size > 0").count === 4)
      assert(df.filter("filepath is null").count === 0)
      assert(df.filter("created_by like 'parquet-mr%'").count === 4)
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
      assert(df.schema === ParquetFileLevel.schema)
      assert(df.count === 1)
    }
  }

  test("set column offset correctly") {
    // File alltypes_plain.snappy.parquet does not seem to set the correct column offset in the
    // metadata correctly, e.g. 55 instead of 4, resulting in Thrift errors when reading pages.
    //
    // This test verifies that no exception is thrown when reading metadata.
    val path = getClass.getResource("/alltypes_plain.snappy.parquet").toString

    readDF.option("level", "file").load(path).foreach(_ => ())
    readDF.option("level", "rowgroup").load(path).foreach(_ => ())
    readDF.option("level", "column").load(path).foreach(_ => ())
    readDF.option("level", "page").load(path).foreach(_ => ())
  }
}
