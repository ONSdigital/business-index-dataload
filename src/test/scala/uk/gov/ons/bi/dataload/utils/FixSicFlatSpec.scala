package uk.gov.ons.bi.dataload.utils

import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

import uk.gov.ons.bi.dataload.reader.LinksFileReader


class FixSicFlatSpec extends FlatSpec with Matchers {

  val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate()
  val ctxMgr = new ContextMgr(sparkSession)
  val parquetReader = new LinksFileReader(ctxMgr)
  val homeDir = parquetReader.readFromLocal("/")

  "createValidSicList" should "contain all valid SICs that are present in the sicCodeIndex" in {

    val sicList = FixSic.createValidSicList(s"$homeDir/sicCodeIndex2017.csv")
    val result = sicList.length
    // Expected is taken from the 15,592 valid SICs published by ONS at
    val expected = 15592

    result shouldBe expected

  }

  "addValidNonTradingSic" should "add valid Non Trading Sics to the existing sic list" in {
    val sicList = FixSic.createValidSicList(s"$homeDir/sicCodeIndex2017.csv")
    val result = FixSic.addValidNonTradingSic(sicList).length
    // plus the 4 valid SICs for non-trading companies (74990, 98000, 99000, 99999)
    val expected = 15596

    result shouldBe expected
  }

  "replaceIncorrectSic" should "be replaced by a valid SIC code" in {
    val sicList = FixSic.createValidSicList(s"$homeDir/sicCodeIndex2017.csv")
    val testString = "1234a"
    val results = FixSic.replaceIncorrectSic(testString, sicList)

    results shouldBe "99999 - 1234a"
  }

  "replaceIncorrectSic" should "when given a valid sic be replaced based on IDBR methodology" in {
    val sicList = FixSic.createValidSicList(s"$homeDir/sicCodeIndex2017.csv")
    val testString = "33120 - tester"
    val results = FixSic.replaceIncorrectSic(testString, sicList)

    results shouldBe "28302 - tester"
  }

  "fixFourDigitSic" should "have a leading zero appended to it" in {

    val testString = "1234"
    val results = FixSic.fixFourDigitSic(testString)

    results shouldBe "01234"
  }

  "fixFourDigitSic" should "do nothing" in {

    val testString = "12345"
    val results = FixSic.fixFourDigitSic(testString)

    results shouldBe "12345"
  }

}
