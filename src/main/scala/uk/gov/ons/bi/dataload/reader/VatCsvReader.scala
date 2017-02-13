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

  import sqlContext.implicits._

  val rawTable = "raw_vat"
  /*
  VAT record fields:
  entref,vatref,deathcode,birthdate,deathdate,sic92,turnover,turnover_date,record_type,legalstatus,actiondate,crn,
  marker,addressref,inqcode,nameline,nameline2,nameline3,tradstyle1,tradstyle2,tradstyle3,
  address1,address2,address3,address4,address5,postcode
   */
  
  /*
    override def build: VatRecord = VatRecord(
    entref = map("entref"),
    vatref = map("vatref"),
    deathcode = map("deathcode"),
    birthdate = map("birthdate"),
    deathdate = map("deathdate"),
    sic92 = map("sic92"),
    turnover = map("turnover"),
    turnover_date = map("turnover_date"),
    record_type = map("record_type"),
    legalstatus = map("legalstatus"),
    actiondate = map("actiondate"),
    crn = map("crn"),
    marker = map("marker"),
    addressref = map("addressref"),
    inqcode = map("inqcode")

   */
  def extractRequiredFields(df: DataFrame): DataFrame = {
    // assume CH structure is correct

    df.registerTempTable(rawTable)

    val extract: DataFrame = sqlContext.sql(
    s"""
       |SELECT `entref`,`vatref`,`deathcode`,`birthdate`,`deathdate`,`sic92`,`turnover`,
       |`turnover_date`,`record_type`,`legalstatus`,`actiondate`,`crn`,`marker`,
       |`addressref`,`inqcode`
       |FROM ${rawTable}
       |""".stripMargin)
/*

    val vatRecs = extract.map{
      case Row(
      entref: String,
      vatref: String,
      deathcode: String,
      birthdate: String,
      deathdate: String,
      sic92: String,
      turnover: String,
      turnover_date: String,
      record_type: String,
      legalstatus: String,
      actiondate: String,
      crn: String,
      marker: String,
      addressref: String,
      inqcode: String
      ) =>
        VatRecord(
          entref,
          vatref,
          deathcode,
          birthdate,
          deathdate,
          sic92,
          turnover,
          turnover_date,
          record_type,
          legalstatus,
          actiondate,
          crn,
          marker,
          addressref,
          inqcode
        )
    }.toDF
*/

    extract
  }

  def testParquetQuery(fileLoc: String)= {

    val df = sqlContext.sql(s"SELECT * FROM parquet.`${fileLoc}`")

    df
  }
}
