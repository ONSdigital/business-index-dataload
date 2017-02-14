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
  /*
  PAYE record fields:
 entref,payeref,deathcode,birthdate,deathdate,mfullemp,msubemp,ffullemp,fsubemp,unclemp,unclsubemp,
 dec_jobs,mar_jobs,june_jobs,sept_jobs,jobs_lastupd,
 legalstatus,prevpaye,employer_cat,stc,crn,actiondate,addressref,marker,
 inqcode,nameline1,nameline2,nameline3,tradstyle1,tradstyle2,tradstyle3,
 address1,address2,address3,address4,address5,postcode
   */

  def extractRequiredFields(df: DataFrame) = {
    // assume CH structure is correct

    df.registerTempTable(rawTable)

    val extract = sqlContext.sql(
      s"""
        |SELECT *
        |FROM ${rawTable}
        |""".stripMargin)

    extract
  }
}
