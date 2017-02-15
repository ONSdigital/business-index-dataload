package uk.gov.ons.bi.dataload.reader

import com.google.inject.Singleton
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame}

/**
  * Created by websc on 08/02/2017.
  */
@Singleton
class CompaniesHouseCsvReader(implicit sc: SparkContext)
  extends CsvReader {

  val rawTable = "raw_companies"

  def extractRequiredFields(df: DataFrame) = {

    df.registerTempTable("raw_companies")

    val extract = sqlContext.sql(
      """
        |SELECT *
        |FROM raw_companies
        |""".stripMargin)

    extract
  }
}
