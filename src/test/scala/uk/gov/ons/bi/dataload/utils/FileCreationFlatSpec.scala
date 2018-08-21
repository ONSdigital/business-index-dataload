package uk.gov.ons.bi.dataload.utils

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{FlatSpec, Matchers}
import java.io.File

import uk.gov.ons.bi.dataload.linker.LinkedBusinessBuilder
import uk.gov.ons.bi.dataload.loader.SourceDataToParquetLoader
import uk.gov.ons.bi.dataload.ubrn.{LinksPreprocessor, UbrnManager}
import uk.gov.ons.bi.dataload.model._

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

    // get input path
    val linksDir = appConfig.OnsDataConfig.linksDataConfig.dir
    val linksFile = appConfig.OnsDataConfig.linksDataConfig.parquet
    val inputFilePath: String  = s"$linksDir/$linksFile"

    // get output path
    val outputDir = appConfig.AppDataConfig.workingDir
    val outputFile = appConfig.AppDataConfig.links
    val outputFilePath: String = s"$outputDir/$outputFile"

    // delete and create output parquet
    new File(outputFilePath).delete()
    new LinksPreprocessor(ctxMgr).loadAndPreprocessLinks(appConfig)

    // Used to create initial input parquet file
    val jsonPath = s"$linksDir/links.json"
    sparkSession.read.json(jsonPath).write.mode("overwrite").parquet(inputFilePath)

    //val result = new File(outputFilePath).exists

    // Read in created parquet
    val results = sparkSession.read.parquet(outputFilePath).collect()

    val expected = Seq(
      (Array("ch1"),Array("065H7Z31732"),Array("868500288000"),1000000000000001L),
      (Array("ch2"),Array(""),Array(""),1000000000000002L),
      (Array(""),Array("035H7A22627"),Array("868504062000"),1000000000000003L),
      (Array("ch3"),Array(""),Array("862764963000"),1000000000000004L),
      (Array("ch4"),Array("125H7A71620"),Array(""),1000000000000005L)
    ).toDF("CH","PAYE","VAT","UBRN").collect()

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
    val appConfig: AppConfig = new AppConfig
    val ctxMgr = new ContextMgr(sparkSession)

    val appDataConfig = appConfig.AppDataConfig
    val workDir = appDataConfig.workingDir
    val parquetBiFile = appDataConfig.bi
    val biFile = s"$workDir/$parquetBiFile"

    new File(biFile).delete

    LinkedBusinessBuilder.buildLinkedBusinessIndexRecords(ctxMgr, appConfig)

    val result = new File(biFile).exists
    result shouldBe true
  }

  "LoadBiToEsApp " should "populate elasticsearch with the populated legal units "in {

  }
}
