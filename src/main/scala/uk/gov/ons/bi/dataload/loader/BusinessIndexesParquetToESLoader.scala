package uk.gov.ons.bi.dataload.loader

import org.apache.spark.SparkContext
import org.elasticsearch.spark.sql._
import uk.gov.ons.bi.dataload.reader.ParquetReader
import uk.gov.ons.bi.dataload.utils._

/**
  * Created by websc on 22/02/2017.
  */
object BusinessIndexesParquetToESLoader {

  def loadBIEntriesToES(sc: SparkContext, appConfig: AppConfig) = {

    val esConf = appConfig.ESConfig

    val index = esConf.index

     // read BI entries

    val pqReader = new ParquetReader(sc)

    val biDf = pqReader.getBIEntriesFromParquet(appConfig)

    println(s"BI index file contained ${biDf.count} records.")

    // write Business Index entries to ES

    // Use "id" field for ES "es.mapping.id" property, appears in doc as _id.
    val extraEsConfig = Map("es.mapping.id" -> "id")

    biDf.saveToEs(s"$index/business",extraEsConfig)
  }
}
