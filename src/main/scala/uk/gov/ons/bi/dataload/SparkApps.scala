package uk.gov.ons.bi.dataload


import org.apache.spark.{SparkConf, SparkContext}
import uk.gov.ons.bi.dataload.linker.LinkedBusinessBuilder
import uk.gov.ons.bi.dataload.loader.{BusinessIndexesParquetToESLoader, SourceDataToParquetLoader}
import uk.gov.ons.bi.dataload.ubrn._
import uk.gov.ons.bi.dataload.utils.{AppConfig, ContextMgr}

/**
  * Created by websc on 02/02/2017.
  *
  * Dev local run command e.g. for SourceDataToParquetApp:
  *
  * Memory options may need to be tweaked but not sure how.
  * Jars are for spark-csv package, which cannot be loaded via SBT for some reason.
  * This will be fixed in Spark 2.0, which includes spark-csv by default.
  *
  * spark-submit --class uk.gov.ons.bi.dataload.SourceDataToParquetApp
  * --master local[*]
  * --driver-memory 2G --executor-memory 4G --num-executors 8
  * --driver-java-options "-Xms1g -Xmx5g"
  * --jars ./lib/spark-csv_2.10-1.5.0.jar,./lib/univocity-parsers-1.5.1.jar,./lib/commons-csv-1.1.jar
  * target/scala-2.10/business-index-dataload_2.10-1.0.jar
  *
  */

trait DataloadApp extends App {
  val appConfig: AppConfig = new AppConfig
}

object SourceDataToParquetApp extends DataloadApp {
  val sparkConf = new SparkConf().setAppName("ONS BI Dataload: Load business data files to Parquet")
  val ctxMgr = new ContextMgr(sparkConf)
  val sourceDataLoader = new SourceDataToParquetLoader(ctxMgr)

  sourceDataLoader.loadSourceBusinessDataToParquet(appConfig)
}

object LinkDataApp extends DataloadApp {
  val sparkConf = new SparkConf().setAppName("ONS BI Dataload: Link data for Business Index")
  val ctxMgr = new ContextMgr(sparkConf)
  // Use an object because defining builder as a class causes weird Spark errors here.
  // Pass context stuff explicitly to builder method.
  LinkedBusinessBuilder.buildLinkedBusinessIndexRecords(ctxMgr, appConfig)
}

object LoadBiToEsApp extends DataloadApp {

  /*

    Need to specify ES host node IP address and index name at runtime e.g.:

    spark-submit --class uk.gov.ons.bi.dataload.LoadBiToEsApp
    --driver-memory 2G --executor-memory 2G
    --driver-java-options "-Xms1g -Xmx4g -Dbi-dataload.es.index=bi-dev -Dbi-dataload.es.nodes=127.0.0.1"
    --jars ./lib/elasticsearch-spark_2.10-2.4.4.jar
    target/scala-2.10/business-index-dataload_2.10-1.0.jar
  */

  // Need to configure ES interface on SparkConf, so need to build SparkConf here.

  val sparkConfigInfo = appConfig.SparkConfigInfo
  val sparkConf = new SparkConf().setAppName(sparkConfigInfo.appName)
  sparkConf.set("spark.serializer", sparkConfigInfo.serializer)

  val esConfig = appConfig.ESConfig

  sparkConf.set("es.nodes", esConfig.nodes)
  sparkConf.set("es.port", esConfig.port.toString)
  sparkConf.set("es.net.http.auth.user", esConfig.esUser)
  sparkConf.set("es.net.http.auth.pass", esConfig.esPass)

  // decides either if ES index should be created manually or not
  sparkConf.set("es.index.auto.create", esConfig.autocreate)

  // WAN only bit was taken from Address Index project, not sure if we need it here?
  // IMPORTANT: without this elasticsearch-hadoop will try to access the interlan nodes
  // that are located on a private ip address. This is generally the case when es is
  // located on a cloud behind a public ip.
  // More: https://www.elastic.co/guide/en/elasticsearch/hadoop/master/cloud.html
  // sparkConf.set("es.nodes.wan.only", esConfig.wanOnly)

  // Now we've built the ES SparkConf, let's go to work:
  // Set up the context manager (singleton holding our Spark and SQL/Hive contexts)
  val ctxMgr = new ContextMgr(sparkConf)
  BusinessIndexesParquetToESLoader.loadBIEntriesToES(ctxMgr, appConfig)

}

object PreprocessLinksApp extends DataloadApp {
  // Load Links JSON, preprocess data (apply UBRN etc), write to Parquet.
  val sparkConf = new SparkConf().setAppName("ONS BI Dataload: Apply UBRN rules to Link data")
  val ctxMgr = new ContextMgr(sparkConf)
  val lpp = new LinksPreprocessor(ctxMgr)
  lpp.loadAndPreprocessLinks(appConfig)
}

