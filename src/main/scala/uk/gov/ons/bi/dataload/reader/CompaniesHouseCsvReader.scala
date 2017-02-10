package uk.gov.ons.bi.dataload.reader

import com.google.inject.Singleton
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame}

/**
  * Created by websc on 08/02/2017.
  */
@Singleton
class CompaniesHouseCsvReader(srcDir: String, srcFile: String)
                             (implicit sc: SparkContext)
  extends CsvReader(srcDir, srcFile)(sc) {

  def extractRequiredFields(df: DataFrame) = {
    // assume CH structure is correct

    df.registerTempTable("raw_companies")

    val extract = sqlContext.sql(
      """
        |SELECT `CompanyName`,`CompanyNumber`, `CompanyCategory`, `CompanyStatus`,
        |`RegAddressCountry`,`RegAddressPostCode`,
        |`SICCodeSicText_1`,`SICCodeSicText_2`,`SICCodeSicText_3`,`SICCodeSicText_4`
        |FROM raw_companies
        |""".stripMargin)

    extract
  }

  def queryCompaniesParquet(fileLoc: String) = {

    val df = sqlContext.sql(s"SELECT * FROM parquet.`${fileLoc}` WHERE `CompanyName` LIKE 'A%'")

    df
  }
}
