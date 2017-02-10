package uk.gov.ons.bi.dataload.reader

import com.google.inject.Singleton
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by websc on 08/02/2017.
  */
@Singleton
class VatCsvReader(srcDir:String,srcFile:String)
                  (implicit sc: SparkContext)
  extends CsvReader(srcDir,srcFile)(sc){

  val rawTable = "raw_vat"
  /*
  VAT record fields:
  entref,vatref,deathcode,birthdate,deathdate,sic92,turnover,turnover_date,record_type,legalstatus,actiondate,crn,
  marker,addressref,inqcode,nameline,nameline2,nameline3,tradstyle1,tradstyle2,tradstyle3,
  address1,address2,address3,address4,address5,postcode
   */

  def extractRequiredFields(df: DataFrame) = {
    // assume CH structure is correct

    df.registerTempTable(rawTable)

    val extract = sqlContext.sql(
      s"""
        |SELECT `entref`,`vatref`, `turnover`, `turnover_date`,
        |`record_type`,`legalstatus`,
        |
        |`crn`,`deathcode`,`addressref`,`postcode`
        |FROM ${rawTable}
        |""".stripMargin)

    extract
  }


  def testParquetQuery(fileLoc: String)= {

    val df = sqlContext.sql(s"SELECT * FROM parquet.`${fileLoc}`")

    df
  }
}
