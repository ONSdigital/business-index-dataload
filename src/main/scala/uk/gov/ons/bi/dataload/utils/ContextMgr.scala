package uk.gov.ons.bi.dataload.utils

// HiveContext is needed for UBRN allocation rules. As we are now using Spark 2.x the SparkSession has the required functionality

import com.google.inject.Singleton
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

@Singleton
class ContextMgr(sparkSession: SparkSession = SparkSession.builder.enableHiveSupport.getOrCreate) extends Serializable {

  // Get logger for this app to use:
  // This still logs to Spark Log4j default appenders (console or file), as
  // it is not clear how to specify e.g. a file appender for this app only.
  // But the log name allows us to filter these entries from the main log.
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("BI-DATALOAD")
  log.setLevel(Level.WARN)
  // Suppress logging from outside the app
  org.apache.log4j.LogManager.getRootLogger.setLevel(Level.WARN)

  implicit val sc: SparkContext = sparkSession.sparkContext    // As many rdd functions still rely on a SparkContext I have kept this val in the Class
  implicit val spark: SparkSession = sparkSession              // This value can be used in place of a SqlContext, HiveContext and a SparkConf

}