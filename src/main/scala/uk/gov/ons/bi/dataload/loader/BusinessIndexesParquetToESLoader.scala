package uk.gov.ons.bi.dataload.loader

import com.google.inject.Singleton
import org.apache.spark.SparkContext
import uk.gov.ons.bi.dataload.reader.ParquetReader
import uk.gov.ons.bi.dataload.utils._
import org.elasticsearch.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.elasticsearch.spark.sql._
import org.elasticsearch.spark._

/**
  * Created by websc on 22/02/2017.
  */
object BusinessIndexesParquetToESLoader {

  def loadBIEntriesToES = {

    // Using SparkProvider (taken from Address Indexes) because we have
    // to configure ES interface on SparkConf directly at outset.

    val sc = SparkProvider.sc

    val appConfig = SparkProvider.appConfig

    // check connection to ES ????


    // read BI entries

    val pqReader = new ParquetReader(sc)

    val biDf = pqReader.getBIEntriesFromParquet(appConfig)

    println(s"BI index file contained ${biDf.count} records.")

    // write them to ES
    biDf.saveToEs("bi-dev/business")
  }
}
