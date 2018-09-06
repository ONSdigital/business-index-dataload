package uk.gov.ons.bi.dataload.reader

import uk.gov.ons.bi.dataload.utils.AppConfig

trait BIDataReader {

  def getBiOutput(appConfig: AppConfig): String = {
    val workDir = getAppDataConfig(appConfig, "working")
    val parquetBiFile = getAppDataConfig(appConfig, "bi")
    val biFile = s"$workDir/$parquetBiFile"
    biFile
  }

  def getAppDataConfig(appConfig: AppConfig, configParam: String): String = {
    val appDataConfig = appConfig.AppDataConfig

    val config = configParam match {
      case "tcn"     => appDataConfig.tcn
      case "paye"    => appDataConfig.paye
      case "vat"     => appDataConfig.vat
      case "links"   => appDataConfig.links
      case "bi"      => appDataConfig.bi
      case "working" => appDataConfig.workingDir
      case "ch"      => appDataConfig.ch
      case "env"     => appDataConfig.env
      case "prev"    => appDataConfig.prevDir
    }
    config
  }

  def getExtDir(appConfig: AppConfig): String = {
    val extDataConfig = appConfig.ExtDataConfig
    val extEnv = extDataConfig.env
    val extBaseDir = extDataConfig.dir
    val extDir = s"$extEnv/$extBaseDir"
    extDir
  }

  def getNewLinksPath(appConfig: AppConfig): String = {
    val linksDataConfig = appConfig.OnsDataConfig.linksDataConfig
    val dataDir = linksDataConfig.dir
    val linksFile = linksDataConfig.file
    val linksFilePath = s"$dataDir/$linksFile"
    linksFilePath
  }
}
