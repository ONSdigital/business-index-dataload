package uk.gov.ons.bi.dataload.exports

import org.apache.log4j.Level
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{concat, concat_ws, explode, lit, col}

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

    def stringifyArr(stringArr: Column) = concat(lit("["), concat_ws(",", stringArr), lit("]"))

    def stringify(string: Column) = concat(lit("\""), string, lit("\""))

    def getHMRCOutput(df: DataFrame): DataFrame = {
      val newDF = df.withColumn("arrVar", df("VatRefs").cast(ArrayType(StringType)))
        .withColumn("arrPaye", df("PayeRefs").cast(ArrayType(StringType)))
        .withColumn("VatRef", stringifyArr($"arrVar"))
        .withColumn("PayeRef", stringifyArr($"arrPaye"))

      newDF.select(newDF.columns.map(c => stringify(col(c)).alias(c)): _*)
        .select("id","BusinessName","TradingStyle","PostCode",
          "Address1", "Address2","Address3","Address4", "Address5",
          "IndustryCode","LegalStatus","TradingStatus",
          "Turnover","EmploymentBands","CompanyNo","VatRef","PayeRef")
    }

    def getLegalEntities(df: DataFrame): DataFrame = {
      df.select("id","BusinessName","TradingStyle","PostCode",
          "Address1", "Address2","Address3","Address4", "Address5",
          "IndustryCode","LegalStatus","TradingStatus",
          "Turnover","EmploymentBands","CompanyNo")
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

}
