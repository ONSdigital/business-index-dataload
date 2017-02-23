package uk.gov.ons.bi.dataload.loader

import org.elasticsearch.spark.sql._
import uk.gov.ons.bi.dataload.reader.ParquetReader
import uk.gov.ons.bi.dataload.utils._

/**
  * Created by websc on 22/02/2017.
  */
object BusinessIndexesParquetToESLoader {

  def loadBIEntriesToES = {

    // Using SparkProvider (taken from Address Indexes) because we have
    // to configure ES interface on SparkConf directly at outset.

    val sc = SparkProvider.sc

    val appConfig = SparkProvider.appConfig

    val esConf = appConfig.ESConfig

    val index = esConf.index

     // read BI entries

    val pqReader = new ParquetReader(sc)

    val biDf = pqReader.getBIEntriesFromParquet(appConfig)

    println(s"BI index file contained ${biDf.count} records.")

    // write Business Index entries to ES

    biDf.saveToEs(s"$index/business")
  }
}
