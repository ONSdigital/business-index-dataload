package uk.gov.ons.bi.dataload.utils

/**
  * Created by websc on 22/02/2017.
  */

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Based on ONS Address Index application and modified for BI.
  * Only using this for ElasticSearch stage of data load because
  * ES driver needs to be configured in SparkConf directly.
  * Provide global access to the spark context instance.
  * Also handles the initialization of the spark context
  */
object SparkProvider {

  lazy val appConfig = new AppConfig

  private val sparkConfigInfo = appConfig.SparkConfigInfo

  private val sparkConf = new SparkConf().setAppName(sparkConfigInfo.appName)
  sparkConf.set("spark.serializer", sparkConfigInfo.serializer)

  /*
  sparkConf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
  sparkConf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
*/
  private val esConfig = appConfig.ESConfig

  sparkConf.set("es.nodes", esConfig.nodes)
  sparkConf.set("es.port", esConfig.port.toString)
  sparkConf.set("es.net.http.auth.user", esConfig.esUser)
  sparkConf.set("es.net.http.auth.pass", esConfig.esPass)

  // decides either if ES index should be created manually or not
  sparkConf.set("es.index.auto.create", esConfig.autocreate)

  // IMPORTANT: without this elasticsearch-hadoop will try to access the interlan nodes
  // that are located on a private ip address. This is generally the case when es is
  // located on a cloud behind a public ip. More: https://www.elastic.co/guide/en/elasticsearch/hadoop/master/cloud.html
  //sparkConf.set("es.nodes.wan.only", esConfig.wanOnly)

  lazy val sc = SparkContext.getOrCreate(sparkConf)
  lazy val sqlContext = SQLContext.getOrCreate(sc)

}