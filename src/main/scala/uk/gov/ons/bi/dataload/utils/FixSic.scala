package uk.gov.ons.bi.dataload.utils

import org.apache.spark.sql.functions.udf

import uk.gov.ons.bi.dataload.reader.LinksFileReader

object FixSic {

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

  def createValidSicList(ctxMgr: ContextMgr, filepath: String): Seq[String] = {
    val reader = new LinksFileReader(ctxMgr)
    val sicCodeIndex = reader.readSimpleCsv(filepath)
    sicCodeIndex.select("SIC 2007").collect().map(x => x.toString()).toSeq.map(x => x.replaceAll("[\\[\\]]",""))
  }

  def addValidNonTradingSic(sicList: Seq[String]): Seq[String] = sicList ++ validNonTradingSic

  def replaceIncorrectSic(sicList: Seq[String]) = udf((sicField: String) => {

    val space = sicField.split(" ")
    val sicDigits = space(0)
    val fourDigitPattern = "(?<!\\d)\\d{4}(?!\\d)".r.pattern
    val fiveDigitPattern = "(?<!\\d)\\d{5}(?!\\d)".r.pattern

    val amendedFourDigitSic = if (fourDigitPattern.matcher(sicDigits).matches) "0" + space(0) else space(0)

    if (amendedFourDigitSic.length == 0) space(0)
      else if (!fiveDigitPattern.matcher(amendedFourDigitSic).matches) space(0) = "99999 - " + amendedFourDigitSic
      else if (!(sicList contains amendedFourDigitSic)) space(0) = "99999"
      else space(0) = amendedFourDigitSic match {
        case "33120" => "28302"
        case "28960" => "28990"
        case "26301" => "27900"
        case "33200" => "26309"
        case "32500" => "26701"
        case "96090" => "33190"
        case "77210" => "77299"
        case "81210" => "81299"
        case _ => amendedFourDigitSic
      }
    space.mkString(" ")
  })

}
