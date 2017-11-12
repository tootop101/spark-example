name := "SparkSas7BDatExample"

version := "0.1"

scalaVersion := "2.10.6"

updateOptions := updateOptions.value.withCachedResolution(true)

resolvers ++= Seq(
  "Maven Central Server" at "http://repo1.maven.org/maven2",
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Spark Packages" at "https://dl.bintray.com/spark-packages/maven/"
)

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.0-cdh5.13.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.0-cdh5.13.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "2.6.0-mr1-cdh5.13.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.0-cdh5.13.0"

libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.5.0"
libraryDependencies += "saurfang" % "spark-sas7bdat" % "1.1.4-s_2.10"
