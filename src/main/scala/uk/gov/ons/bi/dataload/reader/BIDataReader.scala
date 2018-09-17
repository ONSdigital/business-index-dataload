package uk.gov.ons.bi.dataload.reader

import uk.gov.ons.bi.dataload.utils.AppConfig

trait BIDataReader {

  def getBiOutput(appConfig: AppConfig): String = {

    val home = appConfig.BusinessIndex.biPath
    val workDir = appConfig.BusinessIndex.workingDir
    val parquetBiFile = appConfig.BusinessIndex.bi
    s"$home/$workDir/$parquetBiFile"
  }

  def getNewLinksPath(appConfig: AppConfig): String = {

    val home = appConfig.home.env
    val newLinksDir = appConfig.BusinessIndex.dataScienceDir
    val newLinksFile = appConfig.BusinessIndex.dataScienceFile
    s"$home/$newLinksDir/$newLinksFile"
  }
}
