package uk.gov.ons.bi.dataload.writer

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import uk.gov.ons.bi.dataload.utils.ContextMgr
import org.apache.spark.sql.DataFrame

object MetricsWriter {

  def writeMetrics(metricsPath: String, input: String, output: String,
                   chDF: DataFrame, vatDF: DataFrame, payeDF: DataFrame,
                   ctxMgr: ContextMgr, timestamped: Boolean = false) ={

    val spark = ctxMgr.spark
    import spark.implicits._

    // Use timestamp as YYYYMMDD
    val ts = if (timestamped) {
      val fmt = DateTimeFormat.forPattern("yyyyMMddHHmm")

      val now = DateTime.now()
      now.toString(fmt)
    }
    else "latest"

    val inputCount = spark.read.parquet(input).count()
    val outputCount = spark.read.parquet(output).count()

    val metricsFile = s"$metricsPath/$ts.csv"

    val metricsDF = Seq(
      ("DataScience", inputCount),
      ("DataIngestion", outputCount),
      ("CH", chDF.count),
      ("VAT", vatDF.count),
      ("Live VATs", vatDF.filter("deathcode = 0").count,
      ("PAYE", payeDF.count),
      ("Live PAYEs", payeDF.filter("deathcode = 0").count))
    ).toDF("MetricType", "Count")

    BiCsvWriter.writeCsvOutput(metricsDF, metricsFile)
  }
}
