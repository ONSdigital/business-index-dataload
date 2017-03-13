package uk.gov.ons.bi.dataload


import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import uk.gov.ons.bi.dataload.linker.LinkedBusinessBuilder
import uk.gov.ons.bi.dataload.loader.{BusinessIndexesParquetToESLoader, SourceDataToParquetLoader}
import uk.gov.ons.bi.dataload.reader.LinkJsonReader
import uk.gov.ons.bi.dataload.utils.AppConfig
import uk.gov.ons.bi.dataload.ubrn._

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

  val sc: SparkContext

  val appConfig: AppConfig = new AppConfig

}

object SourceDataToParquetApp extends DataloadApp {

  val sc: SparkContext = SparkContext.getOrCreate(new SparkConf().setAppName("ONS BI Dataload: Load business data files to Parquet"))

  val sourceDataLoader = new SourceDataToParquetLoader(sc)

  sourceDataLoader.loadSourceBusinessDataToParquet(appConfig)

}

object LinkDataApp extends DataloadApp {

  val sc = SparkContext.getOrCreate(new SparkConf().setAppName("ONS BI Dataload: Link data for Business Index"))
  // Use an object because defining builder as a class causes weird Spark errors here.
  // Pass SC explicitly to builder method.
  LinkedBusinessBuilder.buildLinkedBusinessIndexRecords(sc, appConfig)

}

object LoadBiToEsApp extends DataloadApp {

  /*

    Need to specify ES host node IP address and index name at runtime:

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

  /*

  DO WE NEED THIS?

  sparkConf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
  sparkConf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
  */
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
  val sc = SparkContext.getOrCreate(sparkConf)

  BusinessIndexesParquetToESLoader.loadBIEntriesToES(sc, appConfig)

}

object PreprocessLinksApp extends DataloadApp {
  // Load Links JSON, preprocess data (apply UBRN etc), write to Parquet.
  val sc = SparkContext.getOrCreate(new SparkConf().setAppName("ONS BI Dataload: Apply UBRN rules to Link data"))
  val lpp = new LinksPreprocessor(sc)
  lpp.loadAndPreprocessLinks(appConfig)

}

