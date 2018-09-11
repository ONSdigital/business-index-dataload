package uk.gov.ons.bi.dataload.utils

/**
  * Created by chohab on 10/09/2018.
  */
object FixSic {

  val subForSic07 = Map {
    "33120" -> "28302"
    "28960" -> "28990"
    "26301" -> "27900"
    "33200" -> "26309"
    "32500" -> "26701"
    "96090" -> "33190"
    "77210" -> "77299"
    "81210" -> "81299s"
  }

  def createValidSicList(filepath: String): Seq[String] = {
    Seq(filepath)
  }

  def replaceIncorrectSic(x: String, sicList: List[String]): String = {
    ""
  }

  def fixFourDigitSic(x: String, sicList: List[String]): String = {
    ""
  }

}
