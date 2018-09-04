package uk.gov.ons.bi.dataload.writer

import org.apache.spark.sql.DataFrame
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object PreviousLinksWriter {

  def writeAsPrevLinks(prevDir: String, linksFile: String, df: DataFrame, timestamped: Boolean = false) = {

    // Use timestamp as YYYYMMDD
    val ts = if (timestamped) {
      val fmt = DateTimeFormat.forPattern("yyyyMMddHHmm")

      val now = DateTime.now()
      now.toString(fmt)
    }
    else ""

    val prevLinksFile = s"$prevDir/$ts/$linksFile"

    // We will also write a copy of the preprocessed Links data to the "previous" dir
    df.write.mode("overwrite").parquet(prevLinksFile)
  }
}
