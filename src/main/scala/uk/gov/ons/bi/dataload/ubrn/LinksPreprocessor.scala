package uk.gov.ons.bi.dataload.ubrn

import java.util.UUID

import com.google.inject.Singleton
import org.apache.spark.sql.DataFrame
import uk.gov.ons.bi.dataload.reader.LinksParquetReader
import uk.gov.ons.bi.dataload.utils.{AppConfig, ContextMgr}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
  * Created by websc on 03/03/2017.
  */

@Singleton
class LinksPreprocessor(ctxMgr: ContextMgr) {


  // Create UDF to generate a UUID
  val generateUuid: UserDefinedFunction = udf(() => UUID.randomUUID().toString)

  def getNewLinksDataFromParquet(reader: LinksParquetReader , appConfig: AppConfig): DataFrame = {

    // get source/target directories
    val linksDataConfig = appConfig.OnsDataConfig.linksDataConfig
    val dataDir = linksDataConfig.dir
    val parquetFile = linksDataConfig.parquet
    val parquetFilePath = s"$dataDir/$parquetFile"

    // Load the JSON links data
    reader.readFromSourceFile(parquetFilePath)
  }

  def loadAndPreprocessLinks(appConfig: AppConfig) = {

    // Lot of caching needed here, so we cache to disk and memory
    // Load the new Links from JSON
    val parquetReader = new LinksParquetReader(ctxMgr)
    val parquetLinks = getNewLinksDataFromParquet(parquetReader, appConfig)
    val withNewUbrn: DataFrame = UbrnManager.applyNewUbrn(parquetLinks)

    val appDataConfig = appConfig.AppDataConfig
    val workingDir = appDataConfig.workingDir
    val linksFile = appDataConfig.links
    val newLinksFileParquetPath = s"$workingDir/$linksFile"

    withNewUbrn.write.mode("overwrite").parquet(newLinksFileParquetPath)
  }

}
