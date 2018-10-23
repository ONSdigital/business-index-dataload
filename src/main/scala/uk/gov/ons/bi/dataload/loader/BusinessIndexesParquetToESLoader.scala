package uk.gov.ons.bi.dataload.loader

import uk.gov.ons.bi.dataload.reader.ParquetReaders
import uk.gov.ons.bi.dataload.utils._
import org.elasticsearch.spark.sql._
import uk.gov.ons.bi.dataload.writer.PreviousLinksWriter

object BusinessIndexesParquetToESLoader {

  def loadBIEntriesToES(ctxMgr: ContextMgr, appConfig: AppConfig) = {

    val esConf = appConfig.ESConfig

    val index = esConf.index

    val indexType = esConf.indexType

    val home = appConfig.BusinessIndex.biPath
    val parquetDir = appConfig.BusinessIndex.elasticDir
    val esOutput = s"$home/$parquetDir"

    val historicPath = appConfig.Historic.historicPath

    // read BI entries

    val pqReader = new ParquetReaders(appConfig, ctxMgr)

    val biDf = pqReader.biParquetReader()

    println(s"BI index file contained ${biDf.count} records.")

    // write Business Index entries to ES

    // Use "id" field for ES "es.mapping.id" property, appears in doc as _id.
    val extraEsConfig = Map("es.mapping.id" -> "id")

    //Write the dataframe out to a file in HDFS
    biDf.write.mode("overwrite").parquet(s"$esOutput")

    PreviousLinksWriter.writeOutputToHistoric(historicPath, biDf)

    biDf.saveToEs(s"$index/$indexType",extraEsConfig)
  }
}
