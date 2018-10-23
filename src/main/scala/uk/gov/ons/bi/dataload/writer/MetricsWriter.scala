package uk.gov.ons.bi.dataload.writer

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import uk.gov.ons.bi.dataload.utils.ContextMgr

object MetricsWriter {

  def writeMetrics(metricsPath: String, input: String, output: String, ctxMgr: ContextMgr) ={
    val spark = ctxMgr.spark
    import spark.implicits._

    val fmt = DateTimeFormat.forPattern("yyyyMMddHHmm")

    val now = DateTime.now()
    val timestamp = now.toString(fmt)

    val inputCount = spark.read.parquet(input).count()
    val outputCount = spark.read.parquet(output).count()
    val metricsFile = s"$metricsPath/$timestamp.csv"

    val metricsDF = Seq(
      ("DataScience", inputCount),
      ("DataIngestion", outputCount)
    ).toDF("MetricType", "Count")

    BiCsvWriter.writeCsvOutput(metricsDF, metricsFile)
  }
}
