package uk.gov.ons.bi.dataload.reader

import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import uk.gov.ons.bi.dataload.model.BiSparkDataFrames
import uk.gov.ons.bi.dataload.model.BiSparkDataFrames._

import scala.util.{Success, Try}

/**
  * Created by websc on 08/03/2017.
  */
class PreviousLinksReader(sc: SparkContext)
  extends BIDataReader(sc: SparkContext) {

  def readFromSourceFile(srcFilePath: String): DataFrame = {
    // If Prev Links not found, returns an empty DataFrame with same schema
    Try {
      sqlContext.read.parquet(srcFilePath)
    }
    match {
      case Success(df: DataFrame) => df
      case _ => BiSparkDataFrames.emptyLinkWithUbrnDf(sc, sqlContext)
    }
  }
}
