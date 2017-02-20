package uk.gov.ons.bi.dataload.writer

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import uk.gov.ons.bi.dataload.model._
import uk.gov.ons.bi.dataload.utils.AppConfig

/**
  * Created by websc on 20/02/2017.
  */
class ParquetWriter (implicit sc: SparkContext) {

  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  object UbrnCompanyRec {
    // Support transforming RDD of these into DataFrame

    val dfSchema = StructType(Seq(
      StructField("ubrn", StringType, true),
      StructField("companyNo", StringType, true),
      StructField("companyName", StringType, true),
      StructField("companyStatus", StringType, true),
      StructField("sicCode1", StringType, true),
      StructField("postcode", StringType, true)
    ))

    def dfRowMapper(ubrn: String, rec: CompanyRec): Row = {
      Row(ubrn, rec.companyNo, rec.companyName, rec.companyStatus, rec.sicCode1, rec.postcode)
    }
  }
  object VatRec {
    // Support transforming RDD of these into DataFrame

    val dfSchema = StructType(Seq(
      StructField("vatRef", LongType, true),
      StructField("nameLine1", StringType, true),
      StructField("postcode", StringType, true),
      StructField("sic92", IntegerType, true),
      StructField("legalStatus", IntegerType, true),
      StructField("turnover", LongType, true)
    ))

    def dfRowMapper(rec: VatRec): Row = {
      Row(rec.vatRef, rec.nameLine1, rec.postcode, rec.sic92, rec.legalStatus, rec.turnover)
    }

  }

  object PayeRec {
    // Support transforming RDD of these into DataFrame

    val dfSchema = StructType(Seq(
      StructField("payeRef", StringType, true),
      StructField("nameLine1", StringType, true),
      StructField("postcode", StringType, true),
      StructField("legalStatus", IntegerType, true),
      StructField("decJobs", DoubleType, true),
      StructField("marJobs", DoubleType, true),
      StructField("junJobs", DoubleType, true),
      StructField("sepJobs", DoubleType, true),
      StructField("jobsLastUpd", StringType, true)
    ))

    def dfRowMapper(rec: PayeRec): Row = {
      Row(rec.payeRef, rec.nameLine1, rec.postCode, rec.legalStatus,
        rec.decJobs, rec.marJobs, rec.junJobs, rec.sepJobs, rec.jobsLastUpd)
    }

  }

/*

  def convertRddToDf(rdd: RDD[]) = ???

  def writeBusinessElementDfToParquet(df: DataFrame, appConfig: AppConfig) = {
    val biRows: RDD[Row] = businessIndexes.map(biRowMapper)

    val biDf: DataFrame = sqlContext.createDataFrame(biRows, biSchema)


    // **** TEMPORARY:  Write BI data to Parquet file

    val parquetDataConfig = appConfig.ParquetDataConfig
    val parquetPath = parquetDataConfig.dir
    val biFile = s"$parquetPath/BUSINESS_INDEX_OUTPUT.parquet"

    biDf.printSchema()

    biDf.write.mode("overwrite").parquet(biFile)

  }

  def getDataFrameFromParquet(appConfig: AppConfig, src: BIDataSource): DataFrame = {
    // Read Parquet data via SparkSQL

    // Get data directories
    val parquetDataConfig = appConfig.ParquetDataConfig
    val parquetPath = parquetDataConfig.dir
    val parquetData = src match {
      case LINKS => parquetDataConfig.links
      case CH => parquetDataConfig.ch
      case VAT => parquetDataConfig.vat
      case PAYE => parquetDataConfig.paye
    }
    val dataFile = s"$parquetPath/$parquetData"

    sqlContext.read.parquet(dataFile)
  }
*/

}

