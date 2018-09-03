package uk.gov.ons.bi.dataload.reader

import org.apache.spark.sql.DataFrame

import uk.gov.ons.bi.dataload.utils.AppConfig

/**
  * Created by websc on 10/02/2017.
  */
trait BIDataReader {

  def writeParquet(df: DataFrame, targetFilePath: String):Unit = {
    df.write.mode("overwrite").parquet(targetFilePath)
  }

  def getWorkingDir(appConfig: AppConfig): String = {
    val appDataConfig = appConfig.AppDataConfig
    val workingDir = appDataConfig.workingDir
    workingDir
  }

  def getPrevDir(appConfig: AppConfig): String = {
    val appDataConfig = appConfig.AppDataConfig
    val prevDir = appDataConfig.prevDir
    prevDir
  }

  def getLinksFilePath(appConfig: AppConfig): String = {
    val appDataConfig = appConfig.AppDataConfig
    val linksFile = appDataConfig.links
    linksFile
  }

  def getNewLinksPath(appConfig: AppConfig): String = {
    val linksDataConfig = appConfig.OnsDataConfig.linksDataConfig
    val dataDir = linksDataConfig.dir
    val linksFile = linksDataConfig.file
    val linksFilePath = s"$dataDir/$linksFile"
    linksFilePath
  }
}
