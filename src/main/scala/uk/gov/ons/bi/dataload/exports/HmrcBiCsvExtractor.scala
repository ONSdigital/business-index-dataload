package uk.gov.ons.bi.dataload.exports

import org.apache.log4j.Level
import org.apache.spark.sql.types._

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{concat, concat_ws, explode, lit, col}

import uk.gov.ons.bi.dataload.reader.BIEntriesParquetReader
import uk.gov.ons.bi.dataload.utils.{AppConfig, ContextMgr}
import uk.gov.ons.bi.dataload.writer.BiCsvWriter
import uk.gov.ons.bi.dataload.model.DataFrameColumn

/**
  * Created by websc on 29/06/2017.
  */
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

    // MAIN PROCESSING:
    // Read BI data
    val pqReader = new BIEntriesParquetReader(appConfig, ctxMgr)
    val biData = pqReader.loadFromParquet()

    // Cache to avoid re-loading data for each output
    //biData.persist()
    log.info(s"BI index file contains ${biData.count} records.")

    // Extract the different sets of data we want, and write to output files

    val adminEntities = getLeuWithAdminData(biData, hmrcFile)
    log.info(s"Writing ${adminEntities.count} Legal Entities to $hmrcFile")

    val legalEntities = getLegalEntities(biData, legalFile)
    log.info(s"Writing ${legalEntities.count} Legal Entities to $legalFile")

    val vat = getVatExploded(biData, vatFile)
    log.info(s"Writing ${vat.count} VAT entries to $vatFile")

    val paye =  getPayeExploded(biData, payeFile)
    log.info(s"Writing ${paye.count} PAYE entries to $payeFile")

    // Clear cache
    biData.unpersist(false)
  }

  def stringifyArr(stringArr: Column) = concat(lit("["), concat_ws(",", stringArr), lit("]"))

  def getLeuWithAdminData(df: DataFrame, outputPath: String): DataFrame = {

    val vatAndPaye = df
      .withColumn(DataFrameColumn.VatStringArr, df(DataFrameColumn.VatID).cast(ArrayType(StringType)))
      .withColumn(DataFrameColumn.PayeStringArr, df(DataFrameColumn.PayeID).cast(ArrayType(StringType)))

    val stringifyVatAndPaye = vatAndPaye
      .withColumn(DataFrameColumn.VatString, stringifyArr(vatAndPaye(DataFrameColumn.VatStringArr)))
      .withColumn(DataFrameColumn.PayeString, stringifyArr(vatAndPaye(DataFrameColumn.PayeStringArr)))
      .drop(DataFrameColumn.VatID,DataFrameColumn.PayeID,DataFrameColumn.VatStringArr, DataFrameColumn.PayeStringArr)

    val LeuWithAdminData = stringifyVatAndPaye
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

    BiCsvWriter.writeCsvOutput(LeuWithAdminData, outputPath)
    LeuWithAdminData
  }

  def getLegalEntities(df: DataFrame, outputPath: String): DataFrame = {

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

    BiCsvWriter.writeCsvOutput(legalEntities, outputPath)
    legalEntities
  }

  def getVatExploded(df: DataFrame, outputPath: String): DataFrame = {
    // Flatten (ID,List(VAT Refs)) records to (ID,VAT Ref) pairs
    val vat = df.select(DataFrameColumn.ID,DataFrameColumn.VatID)
      .where(col(DataFrameColumn.VatID).isNotNull)
      .withColumn(DataFrameColumn.VatString, explode(col(DataFrameColumn.VatID)))
      .drop(col(DataFrameColumn.VatID))
    BiCsvWriter.writeCsvOutput(vat, outputPath)
    vat
  }

  def getPayeExploded(df: DataFrame, outputPath: String): DataFrame = {

    // Flatten (ID,List(PAYE Refs)) records to (ID,PAYE Ref) pairs
    val paye = df.select(DataFrameColumn.ID,DataFrameColumn.PayeID)
      .where(col(DataFrameColumn.PayeID).isNotNull)
      .withColumn(DataFrameColumn.PayeString, explode(col(DataFrameColumn.PayeID)))
      .drop(col(DataFrameColumn.PayeID))

    BiCsvWriter.writeCsvOutput(paye, outputPath)
    paye
  }

}
