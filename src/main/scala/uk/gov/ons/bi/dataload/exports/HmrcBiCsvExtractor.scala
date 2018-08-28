package uk.gov.ons.bi.dataload.exports

import org.apache.log4j.Level
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions.{explode, concat_ws, concat, lit, col}
import uk.gov.ons.bi.dataload.reader.BIEntriesParquetReader
import uk.gov.ons.bi.dataload.utils.{AppConfig, ContextMgr}
import uk.gov.ons.bi.dataload.writer.BiCsvWriter
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

    val hmrcOutput = getHMRCOutput(biData, hmrcFile)
    log.info(s"Writing ${hmrcOutput.count} Legal Entities to $hmrcFile")

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

  def getHMRCOutput(df: DataFrame, outputPath: String): DataFrame = {
    val arrDF = df
      .withColumn("arrVar", df("VatRefs").cast(ArrayType(StringType)))
      .withColumn("arrPaye", df("PayeRefs").cast(ArrayType(StringType)))

    val dropDF = arrDF
      .withColumn("VatRef", stringifyArr(arrDF("arrVar")))
      .withColumn("PayeRef", stringifyArr(arrDF("arrPaye")))
      .drop("VatRefs","PayeRefs","arrVar", "arrPaye")

    val hmrcOutput = dropDF
      .select("id","BusinessName","TradingStyle", "PostCode",
        "Address1", "Address2","Address3","Address4", "Address5",
        "IndustryCode","LegalStatus","TradingStatus",
        "Turnover","EmploymentBands","CompanyNo","VatRef","PayeRef")
    BiCsvWriter.writeCsvOutput(hmrcOutput, outputPath)
    hmrcOutput
  }

  def getLegalEntities(df: DataFrame, outputPath: String): DataFrame = {
    val legalEntities = df.select("id","BusinessName","TradingStyle",
      "PostCode", "Address1", "Address2","Address3","Address4", "Address5",
      "IndustryCode","LegalStatus","TradingStatus",
      "Turnover","EmploymentBands","CompanyNo")
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
