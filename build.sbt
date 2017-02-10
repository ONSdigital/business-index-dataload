name := """business-index-dataload"""

version := "1.0"

// Cloudera Spark 1.6.0 is still on Scala 2.10
scalaVersion := "2.10.6"

// Change this to another test framework if you prefer
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

lazy val Versions = new {
  val spark = "1.6.0"
  val sparkCsv = "1.5.0"
}

libraryDependencies+= "org.apache.spark" %% "spark-core" % Versions.spark

libraryDependencies+= "org.apache.spark" %% "spark-sql" % Versions.spark

//libraryDependencies+= "com.databricks" %% "spark-csv" % Versions.sparkCsv

// Uncomment to use Akka
//libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.11"

