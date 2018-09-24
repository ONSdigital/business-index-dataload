package uk.gov.ons.bi.dataload

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import uk.gov.ons.bi.dataload.utils.ContextMgr

trait SparkSessionSpec {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark tests")
      .getOrCreate()
  }
  lazy val sc: SparkContext = {
    spark.sparkContext
  }
  val ctxMgr = new ContextMgr(spark)
}
