package uk.gov.ons.bi.dataload.exports

import org.apache.log4j.Level
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions.{explode, concat_ws, concat, lit, col}
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

    // Read BI data
    val pqReader = new BIEntriesParquetReader(ctxMgr)
    val biData = pqReader.loadFromParquet(appConfig)

    // Cache to avoid re-loading data for each output
    //biData.persist()
    log.info(s"BI index file contains ${biData.count} records.")

    // Extract the different sets of data we want, and write to output files

    val adminEntities = getAdminEntities(biData, hmrcFile)
    log.info(s"Writing ${adminEntities.count} Legal Entities to $hmrcFile")

    val legalEntities = getLegalEntities(biData, legalFile)
    log.info(s"Writing ${legalEntities.count} Legal Entities to $legalFile")

    val vat = getVatExploded(biData, vatFile)
    log.info(s"Writing ${vat.count} VAT entries to $vatFile")

    val paye =  getPayeExploded(biData, payeFile)
    log.info(s"Writing ${paye.count} PAYE entries to $payeFile")

    val hmrcOut = getHMRCOutput(biData)
    log.info(s"Writing ${hmrcOut.count} hmrcOut entries to $hmrcFile")
    BiCsvWriter.writeCsvOutput(hmrcOut, hmrcFile)

    // Clear cache
    biData.unpersist(false)
  }

  def stringifyArr(stringArr: Column) = concat(lit("["), concat_ws(",", stringArr), lit("]"))

  def getAdminEntities(df: DataFrame, outputPath: String): DataFrame = {

    val vatAndPaye = df
      .withColumn(DataFrameColumn.VatStringArr, df(DataFrameColumn.VatID).cast(ArrayType(StringType)))
      .withColumn(DataFrameColumn.PayeStringArr, df(DataFrameColumn.PayeID).cast(ArrayType(StringType)))

    val stringifyVatAndPaye = vatAndPaye
      .withColumn(DataFrameColumn.VatString, stringifyArr(vatAndPaye(DataFrameColumn.VatStringArr)))
      .withColumn(DataFrameColumn.PayeString, stringifyArr(vatAndPaye(DataFrameColumn.PayeStringArr)))
      .drop(DataFrameColumn.VatID,DataFrameColumn.PayeID,DataFrameColumn.VatStringArr, DataFrameColumn.PayeStringArr)

    val hmrcOutput = stringifyVatAndPaye
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

    BiCsvWriter.writeCsvOutput(hmrcOutput, outputPath)
    hmrcOutput
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
    val vat = df.select("id","VatRefs")
      .where(df("VatRefs").isNotNull)
      .withColumn("VatRef", explode(df("VatRefs")))
      .drop(df("VatRefs"))
    BiCsvWriter.writeCsvOutput(vat, outputPath)
    vat
  }

  def getPayeExploded(df: DataFrame, outputPath: String): DataFrame = {
    // Flatten (ID,List(PAYE Refs)) records to (ID,PAYE Ref) pairs
    val paye = df.select("id","PayeRefs")
      .where(df("PayeRefs").isNotNull)
      .withColumn("PayeRef", explode(df("PayeRefs")))
      .drop(df("PayeRefs"))
    BiCsvWriter.writeCsvOutput(paye, outputPath)
    paye
  }

}
