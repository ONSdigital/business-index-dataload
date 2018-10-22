package uk.gov.ons.bi.dataload.writer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

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

    val temp = tempFixPayeDupes(biDf3)

    // Write BI DataFrame to Parquet file. We will load it into ElasticSearch separately.
    temp.write.mode("overwrite").parquet(biOutputFile)
  }

  def writeParquet(df: DataFrame, targetFilePath: String):Unit = {
    df.write.mode("overwrite").parquet(targetFilePath)
  }

  def tempFixPayeDupes(df: DataFrame): DataFrame = {
    val explodedPaye = df.withColumn("temp", explode(df("PayeRefs"))).dropDuplicates()

    val collectedNonDupes = explodedPaye.groupBy("id").agg(collect_list("temp") as "PayeRefs")

    val rejoinedPaye = df.drop("PayeRefs").join(collectedNonDupes, Seq("id"), "leftOuter")

    rejoinedPaye.withColumn("PayeRefs", when(rejoinedPaye("PayeRefs").isNull, Array.empty[String]).otherwise(rejoinedPaye("PayeRefs")))
  }
}