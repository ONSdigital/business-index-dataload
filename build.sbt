name := """business-index-dataload"""

version := "1.0"

// Cloudera Spark 1.6.0 is still on Scala 2.10
scalaVersion := "2.10.6"

// Change this to another test framework if you prefer
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

lazy val Versions = new {
  val spark = "1.6.0"
  val sparkCsv = "1.5.0"
  val joda = "2.9.4"
  val jodaConvert = "1.8.1"
  val json4s = "3.5.0"
}

libraryDependencies++= Seq(
  "org.apache.spark" %% "spark-core" % Versions.spark,
  "org.apache.spark" %% "spark-sql" % Versions.spark,
  "com.databricks" %% "spark-csv" % Versions.sparkCsv,
  "joda-time" %  "joda-time" % Versions.joda,
  "org.json4s" %% "json4s-native" % Versions.json4s
)




