package com.github.sadikovi.metadata

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

import org.slf4j.LoggerFactory

/** A DataSource V1 for file metadata */
class DefaultSource
  extends RelationProvider
  with DataSourceRegister
  with Serializable {

  import DefaultSource._

  @transient protected val log = LoggerFactory.getLogger(classOf[DefaultSource])

  override def shortName(): String = "metadata"

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val spark = sqlContext.sparkSession

    val rootPath = {
      val path = new Path(parameters.getOrElse(PATH_OPT,
        throw new IllegalArgumentException("Path is not provided")))
      val fs = path.getFileSystem(spark.sessionState.newHadoopConf)
      fs.makeQualified(path)
    }
    log.info(s"Root path: $rootPath")

    val maxPartitions = parameters.get(MAX_PARTITIONS_OPT) match {
      case Some(v) => v.toInt
      case None => MAX_PARTITIONS_DEFAULT
    }
    log.info(s"Max partitions: $maxPartitions")

    val bufferSize = parameters.get(BUFFER_SIZE_OPT) match {
      case Some(v) => v.toInt
      case None => BUFFER_SIZE_DEFAULT
    }
    log.info(s"Buffer size: $bufferSize")

    // // Select file index based on the underlying data source
    val fileIndex = inferFileIndex(spark, rootPath, parameters)

    // Infer source if not provided
    val source: String = {
      val sourceOpt = parameters.get(SOURCE_OPT).map { v =>
        val value = v.toLowerCase
        require(ALL_SOURCES.contains(value),
          s"Invalid source: $value, expected one of ${ALL_SOURCES.mkString("[", ", ", "]")}")
        value
      }

      sourceOpt match {
        case Some(other) => other
        case None =>
          // Check if it is a Parquet source. We assume that all of the files in Parquet table
          // are valid Parquet, so we just pick the first one and check magic.
          val isParquet = fileIndex.inputFiles.headOption.map { path =>
            ParquetUtils.isParquetFile(spark, new Path(path))
          }.getOrElse(false)

          if (isParquet) {
            SOURCE_PARQUET
          } else {
            SOURCE_FILE
          }
      }
    }
    log.info(s"Source: $source")

    val level: String = parameters.getOrElse(LEVEL_OPT, LEVEL_FILE).toLowerCase
    require(ALL_LEVELS.contains(level),
      s"Invalid level: $level, expected one of ${ALL_LEVELS.mkString("[", ", ", "]")}")
    log.info(s"Level: $level")

    // Verify the combination of source and level
    val metadataLevel = inferMetadataLevel(source, level)
    log.info(s"Metadata level $metadataLevel")

    // Select file format
    inferFileFormat(spark, fileIndex, metadataLevel, maxPartitions, bufferSize)
  }
}

object DefaultSource {
  // Option for path
  val PATH_OPT = "path"

  // Option for max partitions for a resulting DataFrame
  val MAX_PARTITIONS_OPT = "maxparts"
  val MAX_PARTITIONS_DEFAULT = 200

  // Option for buffer size that is used with Parquet (default value is a typical row group size)
  val BUFFER_SIZE_OPT = "buffersize"
  val BUFFER_SIZE_DEFAULT = 128 * 1024 * 1024

  // Source of the input table
  val SOURCE_OPT = "source"
  val SOURCE_FILE = "file"
  val SOURCE_PARQUET = "parquet"
  val ALL_SOURCES = Seq(SOURCE_FILE, SOURCE_PARQUET)

  // Defines level of metadata to display.
  // Note that not all sources support all levels
  val LEVEL_OPT = "level"
  val LEVEL_FILE = "file"
  val LEVEL_ROW_GROUP = "rowgroup"
  val LEVEL_COLUMN = "column"
  val LEVEL_PAGE = "page"
  val ALL_LEVELS = Seq(LEVEL_FILE, LEVEL_ROW_GROUP, LEVEL_COLUMN, LEVEL_PAGE)

  /** Infers metadata level */
  def inferMetadataLevel(source: String, level: String): MetadataLevel = {
    def assertUnsupported(source: String, level: String): MetadataLevel = {
      throw new IllegalArgumentException(s"Source '$source' does not support '$level' level")
    }

    source match {
      case SOURCE_FILE => level match {
        case LEVEL_FILE => FileLevel
        case _ => assertUnsupported(source, level)
      }
      case SOURCE_PARQUET => level match {
        case LEVEL_FILE => ParquetFileLevel
        case LEVEL_ROW_GROUP => ParquetRowGroupLevel
        case LEVEL_COLUMN => ParquetColumnLevel
        case LEVEL_PAGE => ParquetPageLevel
        case _ => assertUnsupported(source, level)
      }
      case _ => assertUnsupported(source, level)
    }
  }

  /**
   * Returns file index for the root path.
   * Currently we either return in-memory file index, this can be extended to handle Delta.
   * The final file index might change, see `inferFileFormat` method for more information.
   */
  def inferFileIndex(
      spark: SparkSession,
      path: Path,
      parameters: Map[String, String]): FileIndex = {
    val fileStatusCache = FileStatusCache.getOrCreate(spark)
    new InMemoryFileIndex(spark, Seq(path), parameters, None, fileStatusCache)
  }

  /**
   * Creates file format for level.
   * Can override the original file index for certain levels.
   */
  def inferFileFormat(
      spark: SparkSession,
      fileIndex: FileIndex,
      level: MetadataLevel,
      maxPartitions: Int,
      bufferSize: Int): BaseRelation = {
    level match {
      case FileLevel =>
        new FileMetadataFormat(spark, fileIndex, level, maxPartitions)
      case ParquetFileLevel | ParquetRowGroupLevel | ParquetColumnLevel | ParquetPageLevel =>
        new ParquetMetadataFileFormat(spark, fileIndex, level, maxPartitions, bufferSize)
      case other =>
        throw new IllegalArgumentException(s"No file format for level $other")
    }
  }
}
