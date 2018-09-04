package uk.gov.ons.bi.dataload

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

import uk.gov.ons.bi.dataload.linker.LinkedBusinessBuilder
import uk.gov.ons.bi.dataload.loader.{BusinessIndexesParquetToESLoader, SourceDataToParquetLoader}
import uk.gov.ons.bi.dataload.model._
import uk.gov.ons.bi.dataload.reader.{BIDataReader, ParquetReaders}
import uk.gov.ons.bi.dataload.ubrn.LinksPreprocessor
import uk.gov.ons.bi.dataload.utils.{AppConfig, ContextMgr, Transformers}
import uk.gov.ons.bi.dataload.writer.BiParquetWriter


/**
  * Created by websc on 02/02/2017.
  *
  * Dev local run command e.g. for SourceDataToParquetApp:
  *
  * Memory options may need to be tweaked but not sure how.
  * Jars are for spark-csv package, which cannot be loaded via SBT for some reason.
  * This will be fixed in Spark 2.0, which includes spark-csv by default.
  *
  * spark-submit --class uk.gov.ons.bi.dataload.SourceDataToParquetApp
  * --master local[*]
  * --driver-memory 2G --executor-memory 4G --num-executors 8
  * --driver-java-options "-Xms1g -Xmx5g"
  * --jars ./lib/spark-csv_2.10-1.5.0.jar,./lib/univocity-parsers-1.5.1.jar,./lib/commons-csv-1.1.jar
  * target/scala-2.10/business-index-dataload_2.10-1.0.jar
  *
  */

trait DataloadApp extends App {
  val appConfig: AppConfig = new AppConfig
  val env = appConfig.AppDataConfig.cluster
  val sparkSess = env match {
    case "local" => SparkSession.builder.master("local").appName("Business Index").getOrCreate()
    case "cluster" => SparkSession.builder.appName("ONS BI Dataload: Apply UBRN rules to Link data").enableHiveSupport.getOrCreate
  }
  val ctxMgr = new ContextMgr(sparkSess)
}

object PreprocessLinksApp extends DataloadApp with BIDataReader {

  // Load Links File, preprocess data (apply UBRN etc), write to Parquet.
  val lpp = new LinksPreprocessor(ctxMgr)

  // getFilePaths
  val inputPath = getNewLinksPath(appConfig)
  val workingDir = getWorkingDir(appConfig)
  val prevDir = getPrevDir(appConfig)
  val linksFile = getLinksFilePath(appConfig)

  // load links File
  val newLinks = lpp.readNewLinks(inputPath)
  val prevLinks = lpp.readPrevLinks(workingDir, linksFile)

  // pre-process data
  val linksToSave = lpp.preProcessLinks(newLinks, prevLinks)

  //write to parquet
  lpp.writeToParquet(prevDir, workingDir, linksFile, linksToSave)
}

object SourceDataToParquetApp extends DataloadApp {
  val sourceDataLoader = new SourceDataToParquetLoader(ctxMgr)

  // get input and output paths for admin sources
  val (chInput, chOutput) = sourceDataLoader.getAdminDataPaths(CH, appConfig)
  val (vatInput, vatOutput) = sourceDataLoader.getAdminDataPaths(VAT, appConfig)
  val (payeInput, payeOutput) = sourceDataLoader.getAdminDataPaths(PAYE, appConfig)
  val (tcnInput, tcnOutput) = sourceDataLoader.getAdminDataPaths(TCN, appConfig)

  val listOfAdminSources: Seq[(String, String, String, BusinessDataSource)] = Seq(
    (chInput, chOutput, "temp_ch", CH),
    (vatInput, vatOutput, "temp_vat", VAT),
    (payeInput, payeOutput, "temp_paye", PAYE),
    (tcnInput,tcnOutput, "temp_TCN", TCN)
  )

  // Write admin sources to parquet
  listOfAdminSources.foreach(x => sourceDataLoader.writeAdminToParquet(x._1, x._2, x._3, x._4))
}

object LinkDataApp extends DataloadApp {

  val parquetReader = new ParquetReaders(appConfig, ctxMgr)
  val linkRecsReader: RDD[LinkRec] = parquetReader.linksParquetReader()
  val CHReader: RDD[(String, CompanyRec)] = parquetReader.chParquetReader()
  val VATReader: RDD[(String, VatRec)] = parquetReader.vatParquetReader()
  val PAYEReader: RDD[(String, PayeRec)] = parquetReader.payeParquetReader()

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
  BiParquetWriter.writeBiRddToParquet(ctxMgr, appConfig, businessIndexes)
}

object LoadBiToEsApp extends DataloadApp {

  /*

    Need to specify ES host node IP address and index name at runtime e.g.:

    spark-submit --class uk.gov.ons.bi.dataload.LoadBiToEsApp
    --driver-memory 2G --executor-memory 2G
    --driver-java-options "-Xms1g -Xmx4g -Dbi-dataload.es.index=bi-dev -Dbi-dataload.es.nodes=127.0.0.1"
    --jars ./lib/elasticsearch-spark_2.10-2.4.4.jar
    target/scala-2.10/business-index-dataload_2.10-1.0.jar
  */

  // Need to configure ES interface on SparkSession, so need to build SparkSession here.

  val sparkConfigInfo = appConfig.SparkConfigInfo
  val esConfig = appConfig.ESConfig

  override val sparkSess = SparkSession.builder.appName(sparkConfigInfo.appName).enableHiveSupport
    .config("spark.serializer", sparkConfigInfo.serializer)
    .config("es.nodes", esConfig.nodes)
    .config("es.port", esConfig.port.toString)
    .config("es.net.http.auth.user", esConfig.esUser)
    .config("es.net.http.auth.pass", esConfig.esPass)
    .config("es.index.auto.create", esConfig.autocreate)
    .getOrCreate

  override val ctxMgr = new ContextMgr(sparkSess)

  // this line decides either if ES index should be created manually or not
  // config("es.index.auto.create", esConfig.autocreate)

  // Now we've built the ES SparkSession, let's go to work:
  // Set up the context manager (singleton holding our SparkSession)
  BusinessIndexesParquetToESLoader.loadBIEntriesToES(ctxMgr, appConfig)

}

