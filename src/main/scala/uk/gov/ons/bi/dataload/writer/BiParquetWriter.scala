package uk.gov.ons.bi.dataload.writer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import uk.gov.ons.bi.dataload.model.BusinessIndex
import uk.gov.ons.bi.dataload.utils.ContextMgr

object BiParquetWriter {

  def writeBiRddToParquet(ctxMgr: ContextMgr, biOutputFile: String, biRdd: RDD[BusinessIndex]) = {

    val spark = ctxMgr.spark
    import spark.implicits._

    val biDf: DataFrame = biRdd.toDF

    // Add id field and rename ubrn to UPRN
    val biDf2: DataFrame = biDf.withColumn("id", $"ubrn")
      .withColumnRenamed("ubrn", "UPRN")
      .withColumnRenamed("TurnoverBand", "Turnover")
      .withColumnRenamed("EmploymentBand", "EmploymentBands")

    // Reorder the fields into the correct order
    val biDf3: DataFrame = biDf2.select("id",
      "BusinessName",
      "TradingStyle",
      "UPRN",
      "PostCode",
      "Address1",
      "Address2",
      "Address3",
      "Address4",
      "Address5",
      "IndustryCode",
      "LegalStatus",
      "TradingStatus",
      "Turnover",
      "EmploymentBands",
      "CompanyNo",
      "VatRefs",
      "PayeRefs")

    // Write BI DataFrame to Parquet file. We will load it into ElasticSearch separately.
    biDf3.write.mode("overwrite").parquet(biOutputFile)
  }

  def writeParquet(df: DataFrame, targetFilePath: String):Unit = {
    df.write.mode("overwrite").parquet(targetFilePath)
  }
}