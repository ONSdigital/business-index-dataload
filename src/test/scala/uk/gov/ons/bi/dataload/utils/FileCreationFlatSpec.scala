package uk.gov.ons.bi.dataload.utils

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{FlatSpec, Matchers}
import java.io.File

import org.apache.spark.sql.types._

import uk.gov.ons.bi.dataload.linker.LinkedBusinessBuilder
import uk.gov.ons.bi.dataload.loader.SourceDataToParquetLoader
import uk.gov.ons.bi.dataload.ubrn.LinksPreprocessor
import uk.gov.ons.bi.dataload.model._
import uk.gov.ons.bi.dataload.reader.LinksParquetReader

/**
  * Created by ChiuA on 15/08/2018.
  */

class FileCreationFlatSpec extends FlatSpec with Matchers {

  "A Links File " should "be read in from a parquet file and return a dataframe containing links with UBRNS" in {

    // setup config
    val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate()
    import sparkSession.implicits._
    val appConfig: AppConfig = new AppConfig
    val ctxMgr = new ContextMgr(sparkSession)
    val parquetReader = new LinksParquetReader(ctxMgr)

    val appDataConfig = appConfig.AppDataConfig
    val workingDir = appDataConfig.workingDir
    val linksFile = appDataConfig.links

    val linksDataConfig = appConfig.OnsDataConfig.linksDataConfig
    val dataDir = linksDataConfig.dir
    val parquetFile = linksDataConfig.parquet
    val inputFilePath = s"$dataDir/$parquetFile"
    val outputFilePath = s"$workingDir/$linksFile"

//    val inputFilePath: String  = parquetReader.readFromLocal("/"+appConfig.OnsDataConfig.linksDataConfig.parquet)
//    val outputDir: String = parquetReader.readFromLocal(s"/${appConfig.AppDataConfig.dir}/${appConfig.AppDataConfig.work}")
//    val outputFilePath: String = s"$outputDir/${appConfig.AppDataConfig.links}"

    // Used to create initial input parquet file
    val jsonPath = parquetReader.readFromLocal("/links.json")
    sparkSession.read.json(jsonPath).write.mode("overwrite").parquet(inputFilePath)

    // delete and create output parquet
    new File(outputFilePath).delete()
    new LinksPreprocessor(ctxMgr).readWriteParquet(appConfig, parquetReader, inputFilePath, outputFilePath)

    // Read in created parquet
    val results = sparkSession.read.parquet(outputFilePath).collect()

    val expected = Seq(
      (Array("ch1"), Array("065H7Z31732"), Array(""), 1000000000000001L),
      (Array("08209948"), Array(""), Array("312764963000"), 1000000000000002L),
      (Array(""), Array("035H7A22627"), Array("868504062000"), 1000000000000003L), 
      (Array("ch3"), Array(""), Array("862764963000"), 1000000000000004L), 
      (Array("ch4"), Array("125H7A71620"), Array("123764963000"), 1000000000000005L)
    ).toDF("CH", "PAYE", "VAT", "UBRN").collect()

    results shouldBe expected
  }

  "Admin source files " should "read in and write out as a parquet file for the admin source CH" in {

    val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate()
    import sparkSession.implicits._
    val appConfig: AppConfig = new AppConfig
    val ctxMgr = new ContextMgr(sparkSession)
    val sourceDataLoader = new SourceDataToParquetLoader(ctxMgr)

    // output dir and path
    val workingDir = appConfig.AppDataConfig.workingDir
    val parquetFile = appConfig.AppDataConfig.ch
    val targetFilePath = s"$workingDir/$parquetFile"

    new File(targetFilePath).delete()

    sourceDataLoader.loadBusinessDataToParquet(CH, appConfig)

    val result = new File(targetFilePath).exists
    result shouldBe true
  }

  "Admin source files " should "read in and write out as a parquet file for the admin source PAYE" in {
    val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate()
    val appConfig: AppConfig = new AppConfig
    val ctxMgr = new ContextMgr(sparkSession)
    val sourceDataLoader = new SourceDataToParquetLoader(ctxMgr)

    // output dir and path
    val workingDir = appConfig.AppDataConfig.workingDir
    val parquetFile = appConfig.AppDataConfig.paye
    val targetFilePath = s"$workingDir/$parquetFile"

    new File(targetFilePath).delete()

    sourceDataLoader.loadBusinessDataToParquet(PAYE, appConfig)

    val result = new File(targetFilePath).exists
    result shouldBe true
  }

  "Admin source files " should "read in and write out as a parquet file for the admin source VAT" in {
    val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate()
    val appConfig: AppConfig = new AppConfig
    val ctxMgr = new ContextMgr(sparkSession)
    val sourceDataLoader = new SourceDataToParquetLoader(ctxMgr)

    // output dir and path
    val workingDir = appConfig.AppDataConfig.workingDir
    val parquetFile = appConfig.AppDataConfig.vat
    val targetFilePath = s"$workingDir/$parquetFile"

    new File(targetFilePath).delete()

    sourceDataLoader.loadBusinessDataToParquet(VAT, appConfig)

    val result = new File(targetFilePath).exists
    result shouldBe true
  }

  "Admin source files " should "read in and write out as a parquet file for the admin source TCN-lookup" in {
    val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate()
    val appConfig: AppConfig = new AppConfig
    val ctxMgr = new ContextMgr(sparkSession)
    val sourceDataLoader = new SourceDataToParquetLoader(ctxMgr)

    // output dir and path
    val workingDir = appConfig.AppDataConfig.workingDir
    val parquetFile = appConfig.AppDataConfig.tcn
    val targetFilePath = s"$workingDir/$parquetFile"

    new File(targetFilePath).delete

    sourceDataLoader.loadTcnToSicCsvLookupToParquet(appConfig)

    val result = new File(targetFilePath).exists
    result shouldBe true
  }

  "LinkDataApp " should "read in ubrn links and admin data and outputs parquet file containing fully populated Legal Units" in {
    val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate()
    import sparkSession.implicits._
    val appConfig: AppConfig = new AppConfig
    val ctxMgr = new ContextMgr(sparkSession)

    val appDataConfig = appConfig.AppDataConfig
    val workDir = appDataConfig.workingDir
    val parquetBiFile = appDataConfig.bi
    val biFile = s"$workDir/$parquetBiFile"

    new File(biFile).delete

    LinkedBusinessBuilder.buildLinkedBusinessIndexRecords(ctxMgr, appConfig)
    val df = sparkSession.read.parquet(biFile)
    val results = df.sort("id")

    val schema = StructType(Array(
      StructField("id", LongType, true),
      StructField("BusinessName", StringType, true),
      StructField("TradingStyle", StringType, true),
      StructField("UPRN", LongType, true),
      StructField("PostCode", StringType, true),
      StructField("IndustryCode", StringType, true),
      StructField("LegalStatus", StringType, true),
      StructField("TradingStatus", StringType, true),
      StructField("Turnover", StringType, true),
      StructField("EmploymentBands", StringType, true),
      StructField("CompanyNo", StringType, true),
      StructField("VatRefs", ArrayType(LongType, true)),
      StructField("PayeRefs", ArrayType(StringType, true)),
      StructField("Address1", StringType, true),
      StructField("Address2", StringType, true),
      StructField("Address3", StringType, true),
      StructField("Address4", StringType, true),
      StructField("Address5", StringType, true)
    ))

    val data = Seq(
      Row(1000000000000002L, "! LTD", "tradstyle1", 1000000000000002L, "LS10 2RU", "99999", "1", "A", "A", null, "08209948", Array(312764963000L), Array(), "METROHOUSE 57 PEPPER ROAD", "HUNSLET", "LEEDS", "YORKSHIRE", null),
      Row(1000000000000004L, "NAME1", "tradstyle1", 1000000000000004L, "postcode", null, "0", null, "A", null, null, Array(862764963000L), Array(), "address1", "address2", "address3", "address4", "address5"),
      Row(1000000000000003L, "NAME1", "tradstyle1", 1000000000000003L, "postcode", null, "0", null, "A", null, null, Array(868504062000L), Array("035H7A22627"), "address1", "address2", "address3", "address4", "address5"),
      Row(1000000000000001L, "NAME1", "tradstyle1", 1000000000000001L, "postcode", null, "0", null, null, null, null, Array(), Array("065H7Z31732"), "address1", "address2", "address3", "address4", "address5"),
      Row(1000000000000005L, "NAME1", "tradstyle1", 1000000000000005L, "postcode", null, "0", null, "A", null, null, Array(123764963000L), Array("125H7A71620"), "address1", "address2", "address3", "address4", "address5")
    )
    
    val expected = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(data),schema).sort("id")

    results.collect() shouldBe expected.collect
  }

  "LoadBiToEsApp " should "populate elasticsearch with the populated legal units "in {

  }
}
