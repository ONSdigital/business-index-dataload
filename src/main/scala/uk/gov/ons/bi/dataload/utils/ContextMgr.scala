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
  // Turns down the logging on the Spark execution to reduce noise
  org.apache.log4j.LogManager.getLogger("org").setLevel(Level.WARN)

  // Log app-specific stuff via this one
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("BI-DATALOAD")
  log.setLevel(Level.INFO)

  implicit val sc: SparkContext = SparkContext.getOrCreate(sparkConf)
  implicit val sqlContext =  SQLContext.getOrCreate(sc)

  // We will use HiveContext instead of SQLContext once environment is fixed
  // implicit val sqlContext = new HiveContext(sc)
}