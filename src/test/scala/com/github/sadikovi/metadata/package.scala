package com.github.sadikovi.metadata

import java.io.{InputStream, OutputStream}
import java.util.UUID

import scala.util.Try

import org.apache.log4j.{Level, Logger}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Workaround to disable Parquet logging (org.apache.parquet), which is very verbose and is not
 * particularly meaningful unless debugging Parquet read/write functionality. Currently test trait
 * is inherited in UnitTestSuite.
 */
trait ParquetLogging {
  val logger = java.util.logging.Logger.getLogger("org.apache.parquet")
  val handlers: Array[java.util.logging.Handler] = logger.getHandlers()
  if (handlers == null || handlers.length == 0) {
    println("[LOGGING] Found no handlers for org.apache.parquet, add no-op logging")
    val handler = new java.util.logging.ConsoleHandler()
    handler.setLevel(java.util.logging.Level.OFF)
    logger.addHandler(handler)
    logger.setLevel(java.util.logging.Level.OFF)
  }
}


/** General Spark base */
trait SparkBase {
  @transient private var _spark: SparkSession = null

  def createSparkSession(): SparkSession

  /** Start (or create) Spark session */
  def startSparkSession(): Unit = {
    stopSparkSession()
    setLoggingLevel(Level.ERROR)
    _spark = createSparkSession()
  }

  /** Stop Spark session */
  def stopSparkSession(): Unit = {
    if (_spark != null) {
      _spark.stop()
    }
    _spark = null
  }

  /**
   * Set logging level globally for all.
   * Supported log levels:
   *      Level.OFF
   *      Level.ERROR
   *      Level.WARN
   *      Level.INFO
   * @param level logging level
   */
  def setLoggingLevel(level: Level) {
    Logger.getLogger("org").setLevel(level)
    Logger.getLogger("akka").setLevel(level)
    Logger.getRootLogger().setLevel(level)
  }

  /** Returns Spark session */
  def spark: SparkSession = _spark

  /** Allow tests to set custom SQL configuration for duration of the closure */
  def withSQLConf(pairs: (String, String)*)(func: => Unit): Unit = {
    val (keys, values) = pairs.unzip
    val currentValues = keys.map(key => Try(spark.conf.get(key)).toOption)
    (keys, values).zipped.foreach(spark.conf.set)
    try func finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => spark.conf.set(key, value)
        case (key, None) => spark.conf.unset(key)
      }
    }
  }

  /** Allow tests to set custom SQL configuration for duration of closure using map of options */
  def withSQLConf(options: Map[String, String])(func: => Unit): Unit = {
    withSQLConf(options.toSeq: _*)(func)
  }
}

/** Spark context with master "local[4]" */
trait SparkLocal extends SparkBase {
  /** Loading Spark configuration for local mode */
  private def localConf: SparkConf = {
    new SparkConf().
      setMaster("local[4]").
      setAppName("spark-local-test").
      set("spark.driver.memory", "512m").
      set("spark.executor.memory", "512m")
  }

  override def createSparkSession(): SparkSession = {
    SparkSession.builder().config(localConf).getOrCreate()
  }
}

trait TestBase {
  // local file system for tests
  val fs = FileSystem.get(new Configuration(false))

  /** Create directories for path recursively */
  def mkdirs(path: String): Boolean = mkdirs(new Path(path))

  def mkdirs(path: Path): Boolean = fs.mkdirs(path)

  /** Create empty file, similar to "touch" shell command, but creates intermediate directories */
  def touch(path: String): Boolean = touch(new Path(path))

  def touch(path: Path): Boolean = {
    fs.mkdirs(path.getParent)
    fs.createNewFile(path)
  }

  /** Delete directory / file with path. Recursive must be true for directory */
  def rm(path: String, recursive: Boolean): Boolean = rm(new Path(path), recursive)

  /** Delete directory / file with path. Recursive must be true for directory */
  def rm(path: Path, recursive: Boolean): Boolean = fs.delete(path, recursive)

  /** Open file for a path */
  def open(path: String): InputStream = open(new Path(path))

  def open(path: Path): InputStream = fs.open(path)

  /** Create file with a path and return output stream */
  def create(path: String): OutputStream = create(new Path(path))

  def create(path: Path): OutputStream = fs.create(path)

  /** Compare two DataFrame objects */
  def checkAnswer(df: DataFrame, expected: DataFrame): Unit = {
    val found = df.collect.sortBy(_.toString)
    val exp = expected.collect.sortBy(_.toString)

    // Method checks value
    def checkValue(v1: Any, v2: Any): Unit = {
      (v1, v2) match {
        case (a1: Seq[_], a2: Seq[_]) =>
          assert(a1.length == a2.length,
            s"Array length mismatch ${a1} (${a1.length}) != ${a2} (${a2.length})")
          val a1sorted = a1.sortBy(_.toString)
          val a2sorted = a2.sortBy(_.toString)
          for (i <- 0 until a1.length) {
            checkValue(a1sorted(i), a2sorted(i))
          }
        case (m1: Map[_, _], m2: Map[_, _]) =>
          assert(m1.size == m2.size, s"Map size mismatch ${m1} (${m1.size}) != ${m2} (${m2.size})")
          checkValue(m1.keys.toSeq, m2.keys.toSeq)
          checkValue(m1.values.toSeq, m2.values.toSeq)
        case (r1: Row, r2: Row) =>
          assert(r1.length == r2.length, s"Row length mismatch ${r1.length} != ${r2.length}")
          for (i <- 0 until r1.length) {
            checkValue(r1.get(i), r2.get(i))
          }
        case (b1: Array[Byte], b2: Array[Byte]) =>
          checkValue(b1.toSeq, b2.toSeq)
        case _ =>
          assert(v1 == v2, s"Value mismatch $v1 != $v2")
      }
    }

    // Method to check rows
    def checkRow(row1: Row, row2: Row): Unit = {
      try {
        checkValue(row1, row2)
      } catch {
        case err: AssertionError =>
          val msg = s"""
          |Error: $err
          |Row 1: ${row1}
          |Row 2: ${row2}
          """.stripMargin
          throw new AssertionError(msg, err)
      }
    }

    assert(
      found.length == exp.length,
      s"""
      |Number of records mismatch: ${found.length} != ${exp.length}
      |Found: ${found.mkString("[", ", ", "]")}
      |Expected: ${exp.mkString("[", ", ", "]")}
      """.stripMargin)
    for (i <- 0 until found.length) {
      checkRow(found(i), exp(i))
    }
  }

  def checkAnswer(df: DataFrame, expected: Seq[Row]): Unit = {
    val sc = df.sqlContext.sparkContext
    checkAnswer(df, df.sqlContext.createDataFrame(sc.parallelize(expected), df.schema))
  }

  /** Create temporary directory on local file system */
  def createTempDir(
      root: String = System.getProperty("java.io.tmpdir"),
      namePrefix: String = "parquet"): Path = {
    val dir = new Path(root + "/" + namePrefix + "/" + UUID.randomUUID.toString)
    fs.mkdirs(dir)
    dir
  }

  /** Execute block of code with temporary hadoop path and path permission */
  def withTempPath(path: Path, permission: Option[FsPermission])(func: Path => Unit): Unit = {
    try {
      if (permission.isDefined) {
        fs.setPermission(path, permission.get)
      }
      func(path)
    } finally {
      fs.delete(path, true)
    }
  }


  /** Execute code block with created temporary directory with provided permission */
  def withTempDir(permission: FsPermission)(func: Path => Unit): Unit = {
    withTempPath(createTempDir(), Some(permission))(func)
  }

  /** Execute code block with created temporary directory */
  def withTempDir(func: Path => Unit): Unit = {
    withTempPath(createTempDir(), None)(func)
  }
}

/** abstract general testing class */
abstract class UnitTestSuite extends AnyFunSuite with Matchers
  with TestBase with ParquetLogging with BeforeAndAfterAll with BeforeAndAfter
