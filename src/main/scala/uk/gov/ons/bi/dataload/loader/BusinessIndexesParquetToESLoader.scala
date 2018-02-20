package uk.gov.ons.bi.dataload.loader

import uk.gov.ons.bi.dataload.reader.BIEntriesParquetReader
import uk.gov.ons.bi.dataload.utils._
import org.elasticsearch.spark.sql._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
  * Created by websc on 22/02/2017.
  */
object BusinessIndexesParquetToESLoader {

  def loadBIEntriesToES(ctxMgr: ContextMgr, appConfig: AppConfig) = {

    val esConf = appConfig.ESConfig

    val index = esConf.index

    val indexType = esConf.indexType

    val parquetDir = esConf.parquetDir

     // read BI entries

    val pqReader = new BIEntriesParquetReader(ctxMgr)

    val biDf = pqReader.loadFromParquet(appConfig)

    println(s"BI index file contained ${biDf.count} records.")

    // write Business Index entries to ES

    // Use "id" field for ES "es.mapping.id" property, appears in doc as _id.
    val extraEsConfig = Map("es.mapping.id" -> "id")

    // Create a timestamp to use with the ElasticSearch output file
    val fmt = DateTimeFormat.forPattern("yyyyMMddHHmm")
    val now = DateTime.now()

    val ts = now.toString(fmt)

    //Write the dataframe out to a file in HDFS with a timestamp in the name
    biDf.write.parquet(s"$parquetDir$ts")

    biDf.saveToEs(s"$index/$indexType",extraEsConfig)
  }
}
