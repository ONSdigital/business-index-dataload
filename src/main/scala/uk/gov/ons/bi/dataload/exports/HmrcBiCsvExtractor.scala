package uk.gov.ons.bi.dataload.exports

import org.apache.log4j.Level
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions.{explode, concat_ws, concat, lit}
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
    val sc = ctxMgr.sc
    val spark = ctxMgr.spark
    import spark.implicits._

    val workingDir = appConfig.AppDataConfig.workingDir
    val extractDir = s"$workingDir/${appConfig.AppDataConfig.extract}"

    val legalFile = s"$extractDir/bi-legal-entities.csv"
    val vatFile = s"$extractDir/bi-vat.csv"
    val payeFile = s"$extractDir/bi-paye.csv"
    val hmrcFile = s"$extractDir/bi-hmrc.csv"

    // Extract data from main data frame

    def getLegalEntities(df: DataFrame): DataFrame = {
      df.select("id","BusinessName","TradingStyle","PostCode",
          "Address1", "Address2","Address3","Address4", "Address5",
          "IndustryCode","LegalStatus","TradingStatus",
          "Turnover","EmploymentBands","CompanyNo","VatRef","PayeRef")
    }

    def getVatExploded(df: DataFrame): DataFrame = {
      // Flatten (ID,List(VAT Refs)) records to (ID,VAT Ref) pairs
      df.select("id","VatRefs")
        .where($"VatRefs".isNotNull)
        .withColumn("VatRef", explode($"VatRefs"))
        .drop($"VatRefs")
    }

    def getPayeExploded(df: DataFrame): DataFrame = {
      // Flatten (ID,List(PAYE Refs)) records to (ID,PAYE Ref) pairs
      df.select("id","PayeRefs")
        .where($"PayeRefs".isNotNull)
        .withColumn("PayeRef", explode($"PayeRefs"))
        .drop($"PayeRefs")
    }

    // MAIN PROCESSING:

    // Read BI data
    val pqReader = new BIEntriesParquetReader(ctxMgr)
    val biData = pqReader.loadFromParquet(appConfig)

    // Cache to avoid re-loading data for each output
    //biData.persist()
    log.info(s"BI index file contains ${biData.count} records.")

    // Extract the different sets of data we want, and write to output files

    getHMRCOutput(biData, hmrcFile)

    val legalEntities = getLegalEntities(biData)
    log.info(s"Writing ${legalEntities.count} Legal Entities to $legalFile")
    BiCsvWriter.writeCsvOutput(legalEntities, legalFile)

    val vat = getVatExploded(biData)
    log.info(s"Writing ${vat.count} VAT entries to $vatFile")
    BiCsvWriter.writeCsvOutput(vat, vatFile)

    val paye =  getPayeExploded(biData)
    log.info(s"Writing ${paye.count} PAYE entries to $payeFile")
    BiCsvWriter.writeCsvOutput(paye, payeFile)

    // Clear cache
    biData.unpersist(false)
  }

  def stringify(stringArr: Column) = concat(lit("["), concat_ws(",", stringArr), lit("]"))

  def getHMRCOutput(df: DataFrame, outputPath: String): DataFrame = {
    val hmrcOutput = df.withColumn("arrVar", df("VatRefs").cast(ArrayType(StringType)))
      .withColumn("arrPaye", df("PayeRefs").cast(ArrayType(StringType)))
      .withColumn("VatRef", stringify(df("arrVar")))
      .withColumn("PayeRef", stringify(df("arrPaye")))
      .select("id","BusinessName","TradingStyle","PostCode",
        "Address1", "Address2","Address3","Address4", "Address5",
        "IndustryCode","LegalStatus","TradingStatus",
        "Turnover","EmploymentBands","CompanyNo","VatRef","PayeRef")
    BiCsvWriter.writeCsvOutput(hmrcOutput, outputPath)
    hmrcOutput
  }

}
