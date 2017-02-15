package uk.gov.ons.bi.dataload.reader

import com.google.inject.Singleton
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame}

/**
  * Created by websc on 08/02/2017.
  */
@Singleton
class PayeCsvReader(implicit sc: SparkContext)
  extends CsvReader{

  val rawTable = "raw_paye"

  def extractRequiredFields(df: DataFrame) = {
    // allows us to include/exclude specific fields here

    df.registerTempTable(rawTable)

    val extract = sqlContext.sql(
      s"""
        |SELECT *
        |FROM ${rawTable}
        |""".stripMargin)

    extract
  }
}
