package uk.gov.ons.bi.dataload.exports

import org.apache.log4j.Level
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, concat, concat_ws, explode, lit}

import uk.gov.ons.bi.dataload.reader.ParquetReaders
import uk.gov.ons.bi.dataload.utils.{AppConfig, ContextMgr}
import uk.gov.ons.bi.dataload.writer.BiCsvWriter
import uk.gov.ons.bi.dataload.model.DataFrameColumn
object HmrcBiCsvExtractor {

  def extractBiToCsv(ctxMgr: ContextMgr, appConfig: AppConfig) = {

    val log = ctxMgr.log
    log.setLevel(Level.INFO)

    val workingDir = appConfig.AppDataConfig.workingDir
    val extractDir = s"$workingDir/${appConfig.AppDataConfig.extract}"

    val legalFile = s"$extractDir/bi-legal-entities.csv"
    val vatFile = s"$extractDir/bi-vat.csv"
    val payeFile = s"$extractDir/bi-paye.csv"
    val hmrcFile = s"$extractDir/bi-hmrc.csv"

    // Read BI data
    val pqReader = new ParquetReaders(appConfig, ctxMgr)
    val biData = pqReader.biParquetReader()

    // Cache to avoid re-loading data for each output
    biData.persist()
    log.info(s"BI index file contains ${biData.count} records.")

    // Extract the different sets of data we want, and write to output files

    val adminEntities = getModifiedLegalEntities(modifyLegalEntities(biData))
    log.info(s"Writing ${adminEntities.count} Legal Entities to $hmrcFile")
    BiCsvWriter.writeCsvOutput(adminEntities, hmrcFile)

    val legalEntities = getLegalEntities(biData)
    log.info(s"Writing ${legalEntities.count} Legal Entities to $legalFile")
    BiCsvWriter.writeCsvOutput(legalEntities, legalFile)

    val vat = getVatExploded(biData)
    log.info(s"Writing ${vat.count} VAT entries to $vatFile")
    BiCsvWriter.writeCsvOutput(vat, vatFile)

    val paye =  getPayeExploded(biData)
    log.info(s"Writing ${paye.count} PAYE entries to $payeFile")
    BiCsvWriter.writeCsvOutput(paye, payeFile)

    val hmrcOut = getHMRCOutput(biData)
    log.info(s"Writing ${hmrcOut.count} hmrcOut entries to $hmrcFile")
    BiCsvWriter.writeCsvOutput(hmrcOut, hmrcFile)

    // Clear cache
    biData.unpersist(false)
  }

  def stringifyArr(stringArr: Column) = concat(lit("["), concat_ws(",", stringArr), lit("]"))

  def modifyLegalEntities(df: DataFrame): DataFrame = {

    val castedDf = df
      .withColumn(DataFrameColumn.VatStringArr, df(DataFrameColumn.VatID).cast(ArrayType(StringType)))
      .withColumn(DataFrameColumn.PayeStringArr, df(DataFrameColumn.PayeID).cast(ArrayType(StringType)))

    castedDf
      .withColumn(DataFrameColumn.VatString, stringifyArr(castedDf(DataFrameColumn.VatStringArr)))
      .withColumn(DataFrameColumn.PayeString, stringifyArr(castedDf(DataFrameColumn.PayeStringArr)))
  }

  def getModifiedLegalEntities(df: DataFrame): DataFrame = {

    val LeuWithAdminData = df
      .select("id",
        "BusinessName",
        "TradingStyle",
        "PostCode",
        "Address1",
        "Address2",
        "Address3",
        "Address4",
        "Address5",
        "IndustryCode",
        "LegalStatus",
        "TradingStatus",
        "Turnover",
        "EmploymentBands",
        "CompanyNo",
        "VatRef",
        "PayeRef")

    LeuWithAdminData
  }

  def getLegalEntities(df: DataFrame): DataFrame = {

    val legalEntities = df.select("id",
      "BusinessName",
      "TradingStyle",
      "PostCode",
      "Address1",
      "Address2",
      "Address3",
      "Address4",
      "Address5",
      "IndustryCode",
      "LegalStatus",
      "TradingStatus",
      "Turnover",
      "EmploymentBands",
      "CompanyNo")

    legalEntities
  }

  def getVatExploded(df: DataFrame): DataFrame = {
    // Flatten (ID,List(VAT Refs)) records to (ID,VAT Ref) pairs
    val vat = df.select(DataFrameColumn.ID,DataFrameColumn.VatID)
      .where(col(DataFrameColumn.VatID).isNotNull)
      .withColumn(DataFrameColumn.VatString, explode(col(DataFrameColumn.VatID)))
      .drop(col(DataFrameColumn.VatID))

    vat
  }

  def getPayeExploded(df: DataFrame): DataFrame = {

    // Flatten (ID,List(PAYE Refs)) records to (ID,PAYE Ref) pairs
    val paye = df.select(DataFrameColumn.ID,DataFrameColumn.PayeID)
      .where(col(DataFrameColumn.PayeID).isNotNull)
      .withColumn(DataFrameColumn.PayeString, explode(col(DataFrameColumn.PayeID)))
      .drop(col(DataFrameColumn.PayeID))

    paye
  }

}
