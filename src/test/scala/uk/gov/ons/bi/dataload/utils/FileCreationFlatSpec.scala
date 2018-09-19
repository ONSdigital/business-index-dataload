package uk.gov.ons.bi.dataload.utils

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{FlatSpec, Matchers}
import uk.gov.ons.bi.dataload.exports.HmrcBiCsvExtractor
import uk.gov.ons.bi.dataload.linker.LinkedBusinessBuilder
import uk.gov.ons.bi.dataload.loader.SourceDataToParquetLoader
import uk.gov.ons.bi.dataload.model._
import uk.gov.ons.bi.dataload.reader.{LinksFileReader, ParquetReaders}
import uk.gov.ons.bi.dataload.ubrn.LinksPreprocessor
import uk.gov.ons.bi.dataload.writer.{BiCsvWriter, BiParquetWriter}

class FileCreationFlatSpec extends FlatSpec with Matchers {

  "A Links File " should "be read in from a parquet file and return a dataframe containing links with UBRNS" in {

    // setup config
    val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate()
    import sparkSession.implicits._
    val appConfig: AppConfig = new AppConfig
    val ctxMgr = new ContextMgr(sparkSession)
    val parquetReader = new LinksFileReader(ctxMgr)

    val homeDir = parquetReader.readFromLocal("/")

    val lpp = new LinksPreprocessor(ctxMgr)
    val inputPath = lpp.getNewLinksPath(appConfig)
    val workingDir = appConfig.BusinessIndex.workPath
    val prevDir = appConfig.BusinessIndex.prevPath
    val linksFile = appConfig.BusinessIndex.links
    val outputFilePath = s"$workingDir/$linksFile"

    // Used to create initial input parquet file
    val jsonPath = homeDir + "/links.json"
    sparkSession.read.json(jsonPath).write.mode("overwrite").parquet(inputPath)

    // delete and create output parquet
    new File(outputFilePath).delete()

    // load links File
    val newLinks = lpp.readNewLinks(inputPath)
    val prevLinks = lpp.readPrevLinks(prevDir, linksFile)

    // pre-process data
    val linksToSave = lpp.preProcessLinks(newLinks, prevLinks)
    linksToSave.show()

    //write to parquet
    lpp.writeToParquet(prevDir, workingDir, linksFile, linksToSave)

    // Read in created parquet
    val results = sparkSession.read.parquet(outputFilePath).collect()

    val expected = Seq(
      (1000000000000001L, Array("ch1"), Array(""), Array("065H7Z31732")),
      (1000000000000002L, Array("08209948"), Array("312764963000"), Array("")),
      (1000000000000003L, Array(""), Array("868504062000"), Array("035H7A22627")),
      (1000000000000004L, Array("ch3"), Array("862764963000"), Array("")),
      (1000000000000005L, Array("ch4"), Array("123764963000"), Array("125H7A71620"))
    ).toDF("UBRN", "CH", "VAT", "PAYE").collect()

    results shouldBe expected
  }

  "Admin source files " should "read in and write out as a parquet file for the admin source CH" in {

    val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate()
    import sparkSession.implicits._
    val appConfig: AppConfig = new AppConfig

    val ctxMgr = new ContextMgr(sparkSession)
    val sourceDataLoader = new SourceDataToParquetLoader(ctxMgr)

    val workingDir = appConfig.BusinessIndex.workPath
    val externalDir = appConfig.External.externalPath

    val inputPath = s"$externalDir/companiesHouse/CH.csv"
    val outputPath = s"$workingDir/CH.parquet"

    new File(outputPath).delete()

    sourceDataLoader.writeAdminToParquet(inputPath, outputPath, "temp_ch", CH)

    val result = sparkSession.read.parquet(outputPath).select("CompanyName", "CompanyNumber").collect()

    val expected = Seq(
      ("! LTD", "08209948")
    ).toDF("CompanyName", "CompanyNumber").collect()

    result shouldBe expected
  }

  "Admin source files " should "read in and write out as a parquet file for the admin source PAYE" in {

    val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate()
    import sparkSession.implicits._
    val appConfig: AppConfig = new AppConfig

    val ctxMgr = new ContextMgr(sparkSession)
    val sourceDataLoader = new SourceDataToParquetLoader(ctxMgr)

    val workingDir = appConfig.BusinessIndex.workPath
    val externalDir = appConfig.External.externalPath

    val inputPath = s"$externalDir/hmrc/paye/PAYE.csv"
    val outputPath = s"$workingDir/PAYE.parquet"

    new File(outputPath).delete()

    sourceDataLoader.writeAdminToParquet(inputPath, outputPath, "temp_paye", PAYE)

    val result = sparkSession.read.parquet(outputPath).select("entref", "payeref").collect()

    val expected = Seq(
      ("123", "065H7Z31732"),
      ("234", "035H7A22627"),
      ("345", "125H7A71620")
    ).toDF("entref", "payeref").collect()

    result shouldBe expected
  }

  "Admin source files " should "read in and write out as a parquet file for the admin source VAT" in {

    val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate()
    import sparkSession.implicits._
    val ctxMgr = new ContextMgr(sparkSession)
    val sourceDataLoader = new SourceDataToParquetLoader(ctxMgr)

    val appConfig: AppConfig = new AppConfig
    val workingDir = appConfig.BusinessIndex.workPath
    val externalDir = appConfig.External.externalPath

    val inputPath = s"$externalDir/hmrc/vat/VAT.csv"
    val outputPath = s"$workingDir/VAT.parquet"

    new File(outputPath).delete()

    sourceDataLoader.writeAdminToParquet(inputPath, outputPath, "temp_vat", VAT)

    val result = sparkSession.read.parquet(outputPath).select("entref", "vatref").collect()

    val expected = Seq(
      ("123", 868500288000L),
      ("234", 868504062000L),
      ("345", 862764963000L),
      ("678", 123764963000L),
      ("890", 312764963000L)
    ).toDF("entref", "payeref").collect()

    result shouldBe expected
  }

  "Admin source files " should "read in and write out as a parquet file for the admin source TCN-lookup" in {

    val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate()
    import sparkSession.implicits._
    val ctxMgr = new ContextMgr(sparkSession)
    val sourceDataLoader = new SourceDataToParquetLoader(ctxMgr)

    val appConfig: AppConfig = new AppConfig
    val workingDir = appConfig.BusinessIndex.workPath
    val externalDir = appConfig.External.externalPath

    val inputPath = s"$externalDir/lookups/tcn-to-sic-mapping.csv"
    val outputPath = s"$workingDir/TCN_TO_SIC_LOOKUP.parquet"

    new File(outputPath).delete()

    sourceDataLoader.writeAdminToParquet(inputPath, outputPath, "temp_TCN", TCN)

    val result = sparkSession.read.parquet(outputPath).take(5)

    val expected = Seq(
      ("0100", "01500"),
      ("0101", "01420"),
      ("0102", "01110"),
      ("0103", "01420"),
      ("0104", "01500")
    ).toDF("TCN", "SIC07").collect()

    result shouldBe expected
  }

  "LinkDataApp " should "read in ubrn links and admin data and outputs parquet file containing fully populated Legal Units" in {
    val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate()
    val appConfig: AppConfig = new AppConfig
    val ctxMgr = new ContextMgr(sparkSession)

    // admin outputs
    val workingDir = appConfig.BusinessIndex.workPath

    val biFile = s"$workingDir/BI_Output.parquet"

    new File(biFile).delete

    val parquetReaders = new ParquetReaders(appConfig, ctxMgr)
    val linkRecsReader: RDD[LinkRec] = parquetReaders.linksParquetReader()
    val CHReader: RDD[(String, CompanyRec)] = parquetReaders.chParquetReader()
    val VATReader: RDD[(String, VatRec)] = parquetReaders.vatParquetReader()
    val PAYEReader: RDD[(String, PayeRec)] = parquetReaders.payeParquetReader()

    val uwks: RDD[UbrnWithKey] = LinkedBusinessBuilder.getLinksAsUwks(linkRecsReader)

    // Cache this data as we will be doing different things to it below
    uwks.cache()

    // Join Links to corresponding company/VAT/PAYE data
    val companyData: RDD[UbrnWithData] = LinkedBusinessBuilder.getLinkedCompanyData(uwks, CHReader)
    val vatData: RDD[UbrnWithData] = LinkedBusinessBuilder.getLinkedVatData(uwks, VATReader)
    val payeData: RDD[UbrnWithData] = LinkedBusinessBuilder.getLinkedPayeData(uwks, PAYEReader)

    // Get Ubrn with admin data
    val ubrnWithData: RDD[UbrnWithData] = companyData ++ vatData ++ payeData
    uwks.unpersist()

    // Now we can group data for same UBRN back together to make Business records
    val businessRecords: RDD[Business] = LinkedBusinessBuilder.convertUwdsToBusinessRecords(ubrnWithData)

    // Now we can convert Business records to Business Index entries
    val businessIndexes: RDD[BusinessIndex] = businessRecords.map(Transformers.convertToBusinessIndex)

    // write BI data to parquet file
    BiParquetWriter.writeBiRddToParquet(ctxMgr, biFile, businessIndexes)

    val df = sparkSession.read.parquet(biFile)
    val results = df.sort("id")

    val data = Seq(
      Row(1000000000000002L, "! LTD", "tradstyle1", 1000000000000002L, "LS10 2RU", "METROHOUSE 57 PEPPER ROAD", "HUNSLET", "LEEDS", "YORKSHIRE", null, "99999", "1", "A", "A", null, "08209948", Array(312764963000L), Array()),
      Row(1000000000000004L, "NAME1", "tradstyle1", 1000000000000004L, "postcode", "address1", "address2", "address3", "address4", "address5", null, "0", null, "A", null, null, Array(862764963000L), Array()),
      Row(1000000000000003L, "NAME1", "tradstyle1", 1000000000000003L, "postcode", "address1", "address2", "address3", "address4", "address5", null, "0", null, "A", null, null, Array(868504062000L), Array("035H7A22627")),
      Row(1000000000000001L, "NAME1", "tradstyle1", 1000000000000001L, "postcode", "address1", "address2", "address3", "address4", "address5", null, "0", null, null, null, null, Array(), Array("065H7Z31732")),
      Row(1000000000000005L, "NAME1", "tradstyle1", 1000000000000005L, "postcode", "address1", "address2", "address3", "address4", "address5", null, "0", null, "A", null, null, Array(123764963000L), Array("125H7A71620"))
    )

    val expected = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(data), TestModel.linkSchema).sort("id")

    results.collect() shouldBe expected.collect
  }

  "HmrcBiExportApp - HMRC" should "output Hmrc combined file containing legal units with admin data" in {
    // read in config - pathing for link data appp output
    val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate()

    val appConfig: AppConfig = new AppConfig
    val workingDir = appConfig.BusinessIndex.workPath
    val extractDir = appConfig.BusinessIndex.extractDir

    //get hmrc output filepath
    val extractPath = s"$workingDir/$extractDir"
    val outputCSV = s"$extractPath/bi-hmrc.csv"

    //get bi data filepath
    val parquetBiFile = appConfig.BusinessIndex.bi
    val biFile = s"$workingDir/$parquetBiFile"

    // read in parquet file from path
    val biData = sparkSession.read.parquet(biFile)

    // generate hmrc csv and read as dataframe
    val modLeu = HmrcBiCsvExtractor.modifyLegalEntities(biData)
    val adminEntities = HmrcBiCsvExtractor.getModifiedLegalEntities(modLeu)
    BiCsvWriter.writeCsvOutput(adminEntities, outputCSV)
    val df = sparkSession.read.option("header", true).csv(outputCSV).sort("id")

    // expected data
    val data = Seq(
      Row("1000000000000002", "! LTD", "tradstyle1", "LS10 2RU", "METROHOUSE 57 PEPPER ROAD", "HUNSLET", "LEEDS", "YORKSHIRE", null, "99999", "1", "A", "A", null, "08209948", "[312764963000]", "[]"),
      Row("1000000000000004", "NAME1", "tradstyle1", "postcode", "address1", "address2", "address3", "address4", "address5", null, "0", null, "A", null, null, "[862764963000]", "[]"),
      Row("1000000000000003", "NAME1", "tradstyle1", "postcode", "address1", "address2", "address3", "address4", "address5", null, "0", null, "A", null, null, "[868504062000]", "[035H7A22627]"),
      Row("1000000000000001", "NAME1", "tradstyle1", "postcode", "address1", "address2", "address3", "address4", "address5", null, "0", null, null, null, null, "[]", "[065H7Z31732]"),
      Row("1000000000000005", "NAME1", "tradstyle1", "postcode", "address1", "address2", "address3", "address4", "address5", null, "0", null, "A", null, null, "[123764963000]", "[125H7A71620]")
    )
    val expected = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(data), TestModel.hmrcSchema).sort("id")

    // test expected against results
    df.collect() shouldBe expected.collect()
  }

  "HmrcBiExportApp - LEU" should "output Hmrc combined file containing legal units" in {
    // read in config - pathing for link data appp output
    val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate()

    val appConfig: AppConfig = new AppConfig
    val workingDir = appConfig.BusinessIndex.workPath
    val extractDir = appConfig.BusinessIndex.extractDir

    //get hmrc output filepath
    val extractPath = s"$workingDir/$extractDir"
    val outputCSV = s"$extractPath/bi-legal-entities.csv"

    //get bi data filepath
    val parquetBiFile = appConfig.BusinessIndex.bi
    val biFile = s"$workingDir/$parquetBiFile"

    // read in parquet file from path
    val biData = sparkSession.read.parquet(biFile)

    // generate hmrc csv and read as dataframe
    val legalEntities = HmrcBiCsvExtractor.getLegalEntities(biData)
    BiCsvWriter.writeCsvOutput(legalEntities, outputCSV)
    val df = sparkSession.read.option("header", true).csv(outputCSV).sort("id")

    // expected data
    val data = Seq(
      Row("1000000000000002", "! LTD", "tradstyle1", "LS10 2RU", "METROHOUSE 57 PEPPER ROAD", "HUNSLET", "LEEDS", "YORKSHIRE", null, "99999", "1", "A", "A", null, "08209948"),
      Row("1000000000000004", "NAME1", "tradstyle1", "postcode", "address1", "address2", "address3", "address4", "address5", null, "0", null, "A", null, null),
      Row("1000000000000003", "NAME1", "tradstyle1", "postcode", "address1", "address2", "address3", "address4", "address5", null, "0", null, "A", null, null),
      Row("1000000000000001", "NAME1", "tradstyle1", "postcode", "address1", "address2", "address3", "address4", "address5", null, "0", null, null, null, null),
      Row("1000000000000005", "NAME1", "tradstyle1", "postcode", "address1", "address2", "address3", "address4", "address5", null, "0", null, "A", null, null)
    )
    val expected = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(data), TestModel.leuSchema).sort("id")

    // test expected against results
    df.collect() shouldBe expected.collect()
  }

  "HmrcBiExportApp - VAT" should "output Hmrc combined file containing VAT data" in {
    // read in config - pathing for link data appp output
    val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate()
    import sparkSession.implicits._

    val appConfig: AppConfig = new AppConfig
    val workingDir = appConfig.BusinessIndex.workPath
    val extractDir = appConfig.BusinessIndex.extractDir

    //get hmrc output filepath
    val extractPath = s"$workingDir/$extractDir"
    val outputCSV = s"$extractPath/bi-vat.csv"

    //get bi data filepath
    val parquetBiFile = appConfig.BusinessIndex.bi
    val biFile = s"$workingDir/$parquetBiFile"

    // read in parquet file from path
    val biData = sparkSession.read.parquet(biFile)

    // generate hmrc csv and read as dataframe
    val vat = HmrcBiCsvExtractor.getVatExploded(biData)
    BiCsvWriter.writeCsvOutput(vat, outputCSV)
    val df = sparkSession.read.option("header", true).csv(outputCSV).sort("id")

    // expected data
    val expected = Seq(
      ("1000000000000002", "312764963000"),
      ("1000000000000003", "868504062000"),
      ("1000000000000004", "862764963000"),
      ("1000000000000005", "123764963000")
    ).toDF("id", "VatRef").sort("id")

    // test expected against results
    df.collect() shouldBe expected.collect()
  }

  "HmrcBiExportApp - PAYE" should "output Hmrc combined file containing PAYE data" in {
    // read in config - pathing for link data appp output
    val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate()
    import sparkSession.implicits._

    val appConfig: AppConfig = new AppConfig
    val workingDir = appConfig.BusinessIndex.workPath
    val extractDir = appConfig.BusinessIndex.extractDir

    //get hmrc output filepath
    val extractPath = s"$workingDir/$extractDir"
    val outputCSV = s"$extractPath/bi-paye.csv"

    //get bi data filepath
    val parquetBiFile = appConfig.BusinessIndex.bi
    val biFile = s"$workingDir/$parquetBiFile"

    // read in parquet file from path
    val biData = sparkSession.read.parquet(biFile)

    // generate hmrc csv and read as dataframe
    val paye = HmrcBiCsvExtractor.getPayeExploded(biData)
    BiCsvWriter.writeCsvOutput(paye, outputCSV)
    val df = sparkSession.read.option("header", true).csv(outputCSV).sort("id")

    // expected data
    val expected = Seq(
      ("1000000000000001", "065H7Z31732"),
      ("1000000000000003", "035H7A22627"),
      ("1000000000000005", "125H7A71620")
    ).toDF("id", "PayeRef")

    // test expected against results
    df.collect() shouldBe expected.collect()
  }
}
