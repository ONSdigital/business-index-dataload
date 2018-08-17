package uk.gov.ons.bi.dataload.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

import uk.gov.ons.bi.dataload.model._
import java.io.File

import uk.gov.ons.bi.dataload.loader.SourceDataToParquetLoader
import uk.gov.ons.bi.dataload.ubrn.UbrnManager

/**
  * Created by ChiuA on 15/08/2018.
  */
class FileCreationFlatSpec extends FlatSpec with Matchers {

  "A Links File " should "be read in from a parquet file and return a dataframe" in {

    // setup config
    val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate()

    val homeDir: String = "/Users/ChiuA/projects/dataload/business-index-dataload/src/main/resources"
    val outputFilePath: String = s"$homeDir/LINKS_Output.parquet"
    val inputFilePath: String  = s"$homeDir/legal_units.parquet"

    // Used to create initial input parquet file
    val jsonPath = s"$homeDir/links.json"

    new File(outputFilePath).delete()

    sparkSession.read.json(jsonPath).write.mode("overwrite").parquet(inputFilePath)

    val df: DataFrame = sparkSession.read.parquet(inputFilePath)

    val withNewUbrn = UbrnManager.applyNewUbrn(df)

    withNewUbrn.write.mode("overwrite").parquet(outputFilePath)

    val result = new File(outputFilePath).exists
    result shouldBe true
  }

  "Admin source files " should "read in and wrote out as a parquet file for the admin source CH" in {

    val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate()
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

  "Admin source files " should "read in and wrote out as a parquet file for the admin source PAYE" in {
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

  "Admin source files " should "read in and wrote out as a parquet file for the admin source VAT" in {
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

//  "Admin source files " should "read in and wrote out as a parquet file for the admin source TCN-lookup" in {
//    val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate()
//    val appConfig: AppConfig = new AppConfig
//    val ctxMgr = new ContextMgr(sparkSession)
//    val sourceDataLoader = new SourceDataToParquetLoader(ctxMgr)
//
//    // output dir and path
//    val workingDir = appConfig.AppDataConfig.workingDir
//    val parquetFile = appConfig.AppDataConfig.tcn
//    val targetFilePath = s"$workingDir/$parquetFile"
//
//    new File(targetFilePath).delete()
//
//    sourceDataLoader.loadTcnToSicCsvLookupToParquet(appConfig)
//
//    val result = new File(targetFilePath).exists
//    result shouldBe true  }

  "LinkDataApp"

  "LoadBiToEsApp"
}
