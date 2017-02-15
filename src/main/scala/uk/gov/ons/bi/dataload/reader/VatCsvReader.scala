package uk.gov.ons.bi.dataload.reader

import com.google.inject.Singleton
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import uk.gov.ons.bi.dataload.model.VatRecord

/**
  * Created by websc on 08/02/2017.
  */


@Singleton
class VatCsvReader(implicit sc: SparkContext)
  extends CsvReader{

  val rawTable = "raw_vat"

  def extractRequiredFields(df: DataFrame): DataFrame = {

    df.registerTempTable(rawTable)

    val extract: DataFrame = sqlContext.sql(
    s"""
       |SELECT *
       |FROM ${rawTable}
       |""".stripMargin)

    extract
  }

}
