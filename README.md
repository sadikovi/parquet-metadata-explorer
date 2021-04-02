# parquet-metadata-explorer
Spark SQL datasource for Parquet metadata explorer, the simpler alternative to parquet-cli/parquet-tools.

Features:
- Supports rich set of metadata for Parquet and basic statistics for any file based source like JSON or CSV.
- Metadata is available as a DataFrame so you can query it easily.
- Can process individual files as well as directories, e.g. Parquet tables.
- Collects Parquet metadata like row group sizes, data page offsets, encodings, etc.
- The library works with Apache Spark OSS and Databricks Runtime.

> Note the repository is built for Spark 3.0 by default,
> see Build section to compile for older Spark versions.

## Usage

View the default file level metadata, works on Parquet, JSON, and CSV:
```scala
spark.read.format("metadata").load("/path/to/table_or_file").show()
```

To view row group, column, and page information for a Parquet table or files, use these commands:
```scala
// View Parquet row group metadata
spark.read.format("metadata").option("level", "rowgroup").load("/path/to/parquet").show()

// View Parquet column chunk metadata
spark.read.format("metadata").option("level", "column").load("/path/to/parquet").show()

// View Parquet page metadata
spark.read.format("metadata").option("level", "page").load("/path/to/parquet").show()
```

If required or source inference does not work as expected, you can also specify the metadata source and level directly:
```scala
spark.read.format("metadata")
  .option("source", "parquet")
  .option("level", "rowgroup")
  .load("/path/to/parquet").show()
```

## Configuration

Supported datasource options:

| Name | Description | Default |
|------|-------------|---------|
| `source` | Specifies the source of the table: `parquet`, or `file` (any other format) | Automatically inferred from the path
| `level` | Shows level of metadata for the `source`. Values are `file` (file metadata), `rowgroup` (Parquet row group metadata), `column` (Parquet column chunk metadata), `page` (Parquet page metadata). Note that not all of the sources support all levels | `file`
| `maxParts` | Defines the number of partitions to use when reading data. For example, if you have hundreds of thousands of files, you can use this option to read all of the data in 2000 partitions instead | `min(200, files.length)`
| `bufferSize` | Sets buffer size in bytes for reading Parquet page level data. This reduces the amount of remote calls to DBFS, S3, WASB, ABFS, etc. It is recommended to use a large value, e.g. 64 MB or 128 MB | `128 MB`, typical row group size
| `pageContent` | Enables page content for the `page` level, available values are `true` or `false`. It is recommended to only enable it when inspecting a particular set of pages | `false`

DataFrame schema for each level is in
[MetadataLevel.scala](./src/main/scala/com/github/sadikovi/metadata/MetadataLevel.scala).

## Build
Run `sbt compile` to compile the code.

If you want to compile the code for Spark 2.4 or earlier, update [build.sbt](./build.sbt) file:
```
scalaVersion := "2.11.7"
...
val defaultSparkVersion = "2.4.0"
```

## Package a jar
Run `sbt package` to build a jar.

If you want to package for Spark 2.4 or earlier, update [build.sbt](./build.sbt) file:
```
scalaVersion := "2.11.7"
...
val defaultSparkVersion = "2.4.0"
```

## Test
Run `sbt test`.
