package uk.gov.ons.bi.dataload

import org.apache.spark.{SparkConf, SparkContext}
import uk.gov.ons.bi.dataload.linker.LinkJoiner
import uk.gov.ons.bi.dataload.loader.SourceDataToParquetLoader
import uk.gov.ons.bi.dataload.utils.AppConfig


/**
  * Created by websc on 02/02/2017.

  Dev local run command e.g. for SourceDataToParquetApp:

  Memory options may need to be tweaked but not sure how.
  Jars are for spark-csv package, which cannot be loaded via SBT for some reason.
  This will be fixed in Spark 2.0, which includes spark-csv by default.

  spark-submit --class uk.gov.ons.bi.dataload.SourceDataToParquetApp
  --master local[*]
  --driver-memory 2G --executor-memory 3G
  --driver-java-options "-Xms1g -Xmx4g"
  --jars ./lib/spark-csv_2.10-1.5.0.jar,./lib/univocity-parsers-1.5.1.jar,./lib/commons-csv-1.1.jar
  target/scala-2.10/business-index-dataload_2.10-1.0.jar

  */
  
object SourceDataToParquetApp {
  // Trying to use implicit voodoo to make SC available
  implicit val sc = SparkContext.getOrCreate(new SparkConf().setAppName("ONS BI Dataload: Load raw data to Parquet"))

  def main(args: Array[String]) {

    val appConfig = new AppConfig

    val sourceDataLoader = new SourceDataToParquetLoader

    sourceDataLoader.loadSourceDataToParquet(appConfig)
  }
}

object LinkDataApp {
  // Trying to use implicit voodoo to make SC available
  implicit val sc = SparkContext.getOrCreate(new SparkConf().setAppName("ONS BI Dataload: Link to Business Data"))

  def main(args: Array[String]) {

    val appConfig = new AppConfig

    val linkJoiner = new LinkJoiner

    linkJoiner.buildJoinedData(appConfig)
  }
}
