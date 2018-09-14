name := """business-index-dataload"""

version := "1.6"

// Building with Scala 2.10 because Cloudera Spark 1.6.0 is still on Scala 2.10
//Now updating to Scala 2.11 as updating to Spark 2.1.0
scalaVersion := "2.11.8"

lazy val Versions = new {
  val spark = "2.1.0"
  val sparkSql = "2.1.1"
  val joda = "2.9.4"
  val jodaConvert = "1.8.1"
  val json4s = "3.5.0"
  val es = "6.0.0"
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % Versions.spark,
  "org.apache.spark" %% "spark-sql" % Versions.sparkSql,
// Restore this when the Cloudera bug is fixed so we can use HiveContext for UBRN rules
//  "org.apache.spark" %% "spark-hive" % Versions.spark % "provided",
  "joda-time" % "joda-time" % Versions.joda,
  "org.joda" % "joda-convert" % Versions.jodaConvert,
  "com.typesafe" % "config" % "1.3.2",
  "org.elasticsearch" %% "elasticsearch-spark-20" % Versions.es  excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)

// ============
// Additional repo resolvers
resolvers ++= Seq(
  "conjars" at "http://conjars.org/repo",
  "Spark Package Main Repo" at "https://dl.bintray.com/spark-packages/maven/"
)



