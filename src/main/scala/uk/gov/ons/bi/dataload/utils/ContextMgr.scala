package uk.gov.ons.bi.dataload.utils

/**
  * Created by websc on 17/05/2017.
  */

// HiveContext is needed for UBRN allocation rules
// import org.apache.spark.sql.hive.HiveContext
import com.google.inject.Singleton
import org.apache.log4j.Level
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by websc on 28/04/2017.
  */
@Singleton
class ContextMgr(sparkConf: SparkConf = new SparkConf()) extends Serializable{

  // Get logger for this app to use:
  // This still logs to Spark Log4j default appenders (console or file), as
  // it is not clear how to specify e.g. a file appender for this app only.
  // But the log name allows us to filter these entries from the main log.
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("BI-DATALOAD")
  log.setLevel(Level.WARN)

  // Suppress logging from outside the app
  org.apache.log4j.LogManager.getRootLogger.setLevel(Level.WARN)

  implicit val sc: SparkContext = SparkContext.getOrCreate(sparkConf)
  implicit val sqlContext =  SQLContext.getOrCreate(sc)

  // We will use HiveContext instead of SQLContext once environment is fixed
  // implicit val sqlContext = new HiveContext(sc)
}