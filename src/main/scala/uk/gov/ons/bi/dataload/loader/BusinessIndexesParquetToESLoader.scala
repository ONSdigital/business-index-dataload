package uk.gov.ons.bi.dataload.loader

import uk.gov.ons.bi.dataload.reader.BIEntriesParquetReader
import uk.gov.ons.bi.dataload.utils._
import org.elasticsearch.spark.sql._

/**
  * Created by websc on 22/02/2017.
  */
object BusinessIndexesParquetToESLoader {

  def loadBIEntriesToES(ctxMgr: ContextMgr, appConfig: AppConfig) = {

    val esConf = appConfig.ESConfig

    val index = esConf.index

    val indexType = esConf.indexType

     // read BI entries

    val pqReader = new BIEntriesParquetReader(ctxMgr)

    val biDf = pqReader.loadFromParquet(appConfig)

    println(s"BI index file contained ${biDf.count} records.")

    // write Business Index entries to ES

    // Use "id" field for ES "es.mapping.id" property, appears in doc as _id.
    val extraEsConfig = Map("es.mapping.id" -> "id")

    biDf.saveToEs(s"$index/$indexType",extraEsConfig)
  }
}
