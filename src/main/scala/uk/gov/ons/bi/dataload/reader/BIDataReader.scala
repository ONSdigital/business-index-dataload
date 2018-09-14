package uk.gov.ons.bi.dataload.reader

import org.apache.log4j.Level

import uk.gov.ons.bi.dataload.utils.{AppConfig, ContextMgr}

trait BIDataReader {

  def getBiOutput(appConfig: AppConfig): String = {

    val home = appConfig.BusinessIndex.biPath
    val workDir = appConfig.BusinessIndex.workingDir
    val parquetBiFile = appConfig.BusinessIndex.bi
    s"$home/$workDir/$parquetBiFile"
  }

  def getNewLinksPath(appConfig: AppConfig, ctxMgr: ContextMgr): String = {

    val log = ctxMgr.log
    log.setLevel(Level.INFO)


    val home = appConfig.home.env
    val newLinksDir = appConfig.BusinessIndex.dataScienceDir
    val newLinksFile = appConfig.BusinessIndex.dataScienceFile

    log.info(home + ": this is the env argument")
    log.info(appConfig.home + ": appconfig home")
    log.info(appConfig.BusinessIndex + ": appconfig BusinessIndex")
    log.info(appConfig.External + ": appconfig external")

    s"$home/$newLinksDir/$newLinksFile"
  }
}
