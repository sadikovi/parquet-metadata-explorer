name := "parquet-metadata-explorer"

organization := "com.github.sadikovi"

scalaVersion := "2.12.12"

spName := "databricks/delta-metadata"

val defaultSparkVersion = "3.0.0"

sparkVersion := sys.props.getOrElse("spark.testVersion", defaultSparkVersion)

val defaultHadoopVersion = "2.7.0"

val hadoopVersion = settingKey[String]("The version of Hadoop to test against.")

hadoopVersion := sys.props.getOrElse("hadoop.testVersion", defaultHadoopVersion)

spAppendScalaVersion := true

spIncludeMaven := false

spIgnoreProvided := true

sparkComponents := Seq("sql")

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion.value % "test" exclude("javax.servlet", "servlet-api") force(),
  "org.apache.spark" %% "spark-core" % sparkVersion.value % "test" exclude("org.apache.hadoop", "hadoop-client"),
  "org.apache.spark" %% "spark-sql" % sparkVersion.value % "test" exclude("org.apache.hadoop", "hadoop-client")
)

// Test dependencies
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.0" % "test",
  "com.novocode" % "junit-interface" % "0.11" % "test"
)

// Check deprecation without manual restart
javacOptions in ThisBuild ++= Seq("-Xlint:unchecked")
scalacOptions in ThisBuild ++= Seq("-unchecked", "-deprecation", "-feature")

// Display full-length stacktraces from ScalaTest
testOptions in Test += Tests.Argument("-oF")
testOptions in Test += Tests.Argument(TestFrameworks.JUnit, "-a", "-v", "+q")

parallelExecution in Test := false

// Skip tests during assembly
test in assembly := {}
// Exclude scala library from assembly
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
