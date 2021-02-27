# parquet-metadata-explorer
Spark SQL datasource for Parquet metadata explorer, the simpler alternative to parquet-cli/parquet-tools.

Features:
- Use Spark SQL to analyse Parquet metadata like row group sizes, data page offets, encodings, etc.
- Collect metadata on all of the files in a Parquet table as DataFrame.
- Gather statistics on Parquet or any file based sources like JSON or CSV.
- Implemented as Spark datasource V1, so the code is compatible with earlier versions of Spark.
- The library works with Apache Spark OSS and Databricks Runtime.

> Note the repository is built for Spark 3.0 by default,
> see Build section to compile for older Spark versions.

Supported datasource options:

| Name | Description | Default |
|------|-------------|---------|
| `source` | Specifies the source of the table: `parquet`, or `file` (any other format) | Inferred from the path
| `level` | Shows level of metadata for the `source`. Values are `file` (file metadata), `rowgroup` (Parquet row group metadata), `column` (Parquet column chunk metadata), `page` (Parquet page metadata). Note that not all of the sources support all levels | `file`
| `maxparts` | Defines the number of partitions to use when reading data. For example, if you have hundreds of thousands of files, you can use this option to read all of the data in 2000 partitions instead | `min(200, files.length)`
| `buffersize` | Sets buffer size in bytes for reading Parquet page level data. This reduces the amount of remote calls to DBFS, S3, WASB, ABFS, etc. It is recommended to use a large value, e.g. 64 MB or 128 MB | `128 MB`, typical row group size

DataFrame columns for each level are in
[MetadataLevel.scala](./src/main/scala/com/github/sadikovi/metadata/MetadataLevel.scala).

## Example

Shows metadata for the default file level, works on Parquet, JSON, and CSV:
```scala
spark.read.format("metadata").load("/path/to/table").show()
```

Shows page information for Parquet:
```scala
spark.read.format("metadata")
  .option("level", "page")
  .load("/path/to/parquet").show()
```

You can also specify the metadata source and level directly:
```scala
spark.read.format("metadata")
  .option("source", "parquet")
  .option("level", "column")
  .load("/path/to/parquet").show()
```

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
