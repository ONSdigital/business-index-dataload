package uk.gov.ons.bi.dataload.utils

import org.apache.spark.sql.SparkSession

object FixSic {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark tests")
      .getOrCreate()
  }

  val colNames = Seq(
    "SICCode.SicText_1",
    "SICCode.SicText_2",
    "SICCode.SicText_3",
    "SICCode.SicText_4"
  )

  val validNonTradingSic = Seq(
    "74990",
    "98000",
    "99000",
    "99999"
  )

  def createValidSicList(filepath: String): Seq[String] = {

    val sicCodeIndex = spark.read.option("header", "true").csv(filepath)
    val sic07 = sicCodeIndex.select("SIC 2007").collect().map(x => x.toString()).toSeq.map(x => x.replaceAll("[\\[\\]]",""))
    sic07
  }

  def addValidNonTradingSic(sicList: Seq[String]): Seq[String] = {

    val newList = sicList ++ validNonTradingSic
    newList
  }

  def replaceIncorrectSic(sicField: String, sicList: Seq[String]): String = {

    val space = sicField.split(" ")
    val sicDigits = space(0)
    val fiveDigitPattern = "(?<!\\d)\\d{5}(?!\\d)".r.pattern

      if (sicDigits.length == 0) space(0)
      else if (!fiveDigitPattern.matcher(sicDigits).matches) space(0) = "99999 - " + sicDigits
      else if (!(sicList contains sicDigits)) space(0) = "99999"
      else space(0) = sicDigits match {
        case "33120" => "28302"
        case "28960" => "28990"
        case "26301" => "27900"
        case "33200" => "26309"
        case "32500" => "26701"
        case "96090" => "33190"
        case "77210" => "77299"
        case "81210" => "81299"
        case _ => sicDigits
      }
    space.mkString(" ")
  }

  def fixFourDigitSic(sicField: String): String = {

    val space = sicField.split(" ")
    val sicDigits = space(0)
    val fourDigitPattern = "(?<!\\d)\\d{4}(?!\\d)".r.pattern

    val test = if (fourDigitPattern.matcher(sicDigits).matches) {
      space(0) = "0" + space(0)
      space.mkString(" ")
    }
    else {
      sicField
    }
    test
  }

}
