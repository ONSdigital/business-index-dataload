package uk.gov.ons.bi.dataload.reader

import com.google.inject.Singleton
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by websc on 09/02/2017.
  */
@Singleton
class LinkBuilder (implicit val sc: SparkContext) {

  val sqlContext = new SQLContext(sc)

  def getVatData(dataFile: String): DataFrame = {
    sqlContext.sql(
      s"""
         |SELECT `entref` AS vat_entref,`vatref`,
         |`legalstatus` AS vat_legalstatus,
         |`addressref` AS vat_addressref,`postcode` AS vat_postcode
         |FROM parquet.`$dataFile`
         |""".stripMargin)
  }

  def getPayeData(dataFile: String): DataFrame = {
    sqlContext.sql(
      s"""
         |SELECT `entref` AS paye_entref,`payeref`,
         |`legalstatus` AS paye_legalstatus,
         |`addressref` AS paye_addressref,`postcode` AS paye_postcode
         |FROM parquet.`$dataFile`
         |""".stripMargin)
  }

  def testQuery: DataFrame = {

    // See if we can join some VAT and PAYE data

    val vatFile =  "/Users/websc/Code/business-index-dataload/temp-data/VAT_Output.parquet"

    val payeFile =  "/Users/websc/Code/business-index-dataload/temp-data/PAYE_Output.parquet"

    val vatDf = getVatData(vatFile)

    vatDf.registerTempTable("vat_data")
    vatDf.show(10)

    val payeDf = getPayeData(payeFile)

    payeDf.show(10)

    payeDf.registerTempTable("paye_data")

    val joinedDf = sqlContext.sql(
      """
        |SELECT `vat_entref`, `paye_entref`,
        |`vat_legalstatus`, `paye_legalstatus`,
        |`vat_postcode`,`paye_postcode`
        |FROM paye_data LEFT OUTER JOIN vat_data
        |ON (vat_postcode = paye_postcode)
      """.stripMargin)

    joinedDf
  }

}
