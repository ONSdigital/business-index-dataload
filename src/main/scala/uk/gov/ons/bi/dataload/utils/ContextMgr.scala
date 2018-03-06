package uk.gov.ons.bi.dataload.utils

/**
  * Created by websc on 17/05/2017.
  */

// HiveContext is needed for UBRN allocation rules however this is already a part of the SparkSession
import com.google.inject.Singleton
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

/**
  * Created by websc on 28/04/2017.
  */
@Singleton
class ContextMgr(sparkSession: SparkSession = SparkSession.builder.enableHiveSupport.getOrCreate) extends Serializable{

  // Get logger for this app to use:
  // This still logs to Spark Log4j default appenders (console or file), as
  // it is not clear how to specify e.g. a file appender for this app only.
  // But the log name allows us to filter these entries from the main log.
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("BI-DATALOAD")
  log.setLevel(Level.WARN)

  // Suppress logging from outside the app
  org.apache.log4j.LogManager.getRootLogger.setLevel(Level.WARN)

  implicit val sc: SparkContext = sparkSession.sparkContext
  implicit val spark: SparkSession = sparkSession

}