package uk.gov.ons.bi.dataload.reader

import org.apache.spark.sql.DataFrame
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import uk.gov.ons.bi.dataload.model.BiSparkDataFrames
import uk.gov.ons.bi.dataload.utils.{AppConfig, ContextMgr}

import scala.util.{Success, Try}

/**
  * Created by websc on 08/03/2017.
  */
class PreviousLinkStore(ctxMgr: ContextMgr) extends BIDataReader {

  val sc = ctxMgr.sc
  val spark = ctxMgr.spark

  def readFromSourceFile(srcFilePath: String): DataFrame = {
    // If Prev Links not found, returns an empty DataFrame with same schema
    Try {
      spark.read.parquet(srcFilePath)
    }
    match {
      case Success(df: DataFrame) => df
      case _ => BiSparkDataFrames.emptyLinkWithUbrnDf(ctxMgr)
    }
  }

  def writeAsPrevLinks(appConfig: AppConfig, df: DataFrame, timestamped: Boolean = false) = {
    // Use timestamp as YYYYMMDD
    val ts = if (timestamped) {
      val fmt = DateTimeFormat.forPattern("yyyyMMddHHmm")

      val now = DateTime.now()
      now.toString(fmt)
    }
    else ""

    // Parquet file locations from configuration (or runtime params)
    val appDataConfig = appConfig.AppDataConfig
    val linksFile = appDataConfig.links
    val prevDir = appDataConfig.prevDir
    val prevLinksFile = s"$prevDir/$ts/$linksFile"

    // We will also write a copy of the preprocessed Links data to the "previous" dir
    df.write.mode("overwrite").parquet(prevLinksFile)
  }
}
