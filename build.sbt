name := """business-index-dataload"""

version := "1.2"

// Building with Scala 2.10 because Cloudera Spark 1.6.0 is still on Scala 2.10
scalaVersion := "2.10.6"

// Change this to another test framework if you prefer
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

lazy val Versions = new {
  val spark = "1.6.0"
  val sparkCsv = "1.5.0"
  val joda = "2.9.4"
  val jodaConvert = "1.8.1"
  val json4s = "3.5.0"
  val es = "2.4.4"
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % Versions.spark % "provided",
  "org.apache.spark" %% "spark-sql" % Versions.spark % "provided",
  "joda-time" % "joda-time" % Versions.joda,
  "org.joda" % "joda-convert" % Versions.jodaConvert,
  "org.elasticsearch" %% "elasticsearch-spark" % Versions.es % "provided" excludeAll ExclusionRule(organization = "javax.servlet")
)

// Spark testing: uses SBT plugin for Spark packages as well (see project.plugins.sbt)

spDependencies += "holdenk/spark-testing-base:1.5.2_0.3.3"

parallelExecution in Test := false

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")


resolvers ++= Seq(
  // allows us to include spark packages (not sure how well this works on Cloudera)
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/",
  "conjars" at "http://conjars.org/repo"
)



