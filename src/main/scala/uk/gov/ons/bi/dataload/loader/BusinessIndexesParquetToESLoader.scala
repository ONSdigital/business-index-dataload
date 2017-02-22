package uk.gov.ons.bi.dataload.loader

import com.google.inject.Singleton
import org.apache.spark.SparkContext
import uk.gov.ons.bi.dataload.reader.ParquetReader
import uk.gov.ons.bi.dataload.utils.AppConfig

/**
  * Created by websc on 22/02/2017.
  */
@Singleton
class BusinessIndexesParquetToESLoader(sc: SparkContext) {

  def loadBIEntriesToES(appConfig: AppConfig) = {

    // initialise ES voodoo?

    val esConfig = appConfig.ESConfig

    println(esConfig)

    // check connection to ES ????


    // read BI entries

    val pqReader = new ParquetReader(sc)

    val BiDf = pqReader.getBIEntriesFromParquet(appConfig)

    // write them to ES

    BiDf.printSchema()

  }
}
