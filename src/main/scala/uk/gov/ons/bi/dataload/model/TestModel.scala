package uk.gov.ons.bi.dataload.model

import org.apache.spark.sql.types._

/**
  * Created by ChiuA on 22/08/2018.
  */
object TestModel {

  val linkSchema = StructType(Array(
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
}
