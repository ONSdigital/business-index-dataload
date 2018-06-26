package uk.gov.ons.bi.dataload.model

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import uk.gov.ons.bi.dataload.utils.ContextMgr

import scala.util.{Failure, Success, Try}

/**
  * Created by websc on 16/03/2017.
  */
object BiSparkDataFrames {

  def isDfEmpty(df: DataFrame): Boolean = {
    // If no first record, then it's empty
    Try {
      df.first()
    }
    match {
      case Success(t) => false
      case Failure(x) => true
    }
  }

  // DataFrame schema for legal unit ("link") with UBRN
  val linkWithUbrnSchema = StructType(Seq(
    StructField("UBRN", LongType, true),
    StructField("CH", ArrayType(StringType), true),
    StructField("VAT", ArrayType(StringType), true),
    StructField("PAYE", ArrayType(StringType), true)
  ))

  def emptyLinkWithUbrnDf(ctxMgr: ContextMgr):DataFrame  = {
    val sc = ctxMgr.sc
    val spark = ctxMgr.spark
    spark.createDataFrame(sc.emptyRDD[Row], linkWithUbrnSchema)
  }

  // DataFrame schema for legal unit ("link") with UBRN and Group ID
  val matchedLinkWithUbrnGidSchema = StructType(Seq(
    StructField("UBRN", LongType, true),
    StructField("GID", StringType, true),
    StructField("CH", ArrayType(StringType), true),
    StructField("VAT", ArrayType(StringType), true),
    StructField("PAYE", ArrayType(StringType), true)
  ))

  def emptyMatchedLinkWithUbrnGidDf(ctxMgr: ContextMgr):DataFrame  = {
    val sc = ctxMgr.sc
    val spark = ctxMgr.spark
    spark.createDataFrame(sc.emptyRDD[Row], matchedLinkWithUbrnGidSchema)
  }

  // Need some voodoo here to convert RDD[BusinessIndex] back to DataFrame.
  // This effectively defines the format of the final BI record in ElasticSearch.

  val biSchema = StructType(Seq(
    StructField("id", LongType, true), // use UBRN here
    StructField("BusinessName", StringType, true),
    StructField("UPRN", LongType, true), // spec says "UPRN", but we use UBRN
    StructField("PostCode", StringType, true),
    StructField("IndustryCode", StringType, true),
    StructField("LegalStatus", StringType, true),
    StructField("TradingStatus", StringType, true),
    StructField("Turnover", StringType, true),
    StructField("EmploymentBands", StringType, true),
    StructField("CompanyNo", StringType, true),
    StructField("VatRefs", ArrayType(LongType), true), // sequence of Long VAT refs
    StructField("PayeRefs", ArrayType(StringType), true) // seq of String PAYE refs
  ))

  // Use UBRN as ID (and as UPRN) in index until we have better information
  def biRowMapper(bi: BusinessIndex): Row = {
    Row(bi.ubrn, bi.businessName, bi.ubrn, bi.postCode, bi.industryCode, bi.legalStatus,
      bi.tradingStatus, bi.turnoverBand, bi.employmentBand, bi.companyNo, bi.vatRefs, bi.payeRefs)
  }
}