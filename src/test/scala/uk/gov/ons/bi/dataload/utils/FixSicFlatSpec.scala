package uk.gov.ons.bi.dataload.utils

import org.scalatest.{FlatSpec, Matchers}

import uk.gov.ons.bi.dataload.SparkSessionSpec
import uk.gov.ons.bi.dataload.helper.DataframeAsserter
import uk.gov.ons.bi.dataload.reader.LinksFileReader


class FixSicFlatSpec extends FlatSpec with Matchers with SparkSessionSpec with DataframeAsserter {

  val parquetReader = new LinksFileReader(ctxMgr)
  val homeDir = parquetReader.readFromLocal("/")
  val sicListPath = "ons.gov/businessIndex/sicIndex/sicCodeIndex2017.csv"
  import spark.implicits._

  "createValidSicList" should "contain all valid SICs that are present in the sicCodeIndex" in {

    val sicList = FixSic.createValidSicList(ctxMgr, s"$homeDir/$sicListPath")
    val result = sicList.length
    // Expected is taken from the 15,592 valid SICs published by ONS at
    val expected = 15592

    result shouldBe expected

  }

  "addValidNonTradingSic" should "add valid Non Trading Sics to the existing sic list" in {
    val sicList = FixSic.createValidSicList(ctxMgr, s"$homeDir/$sicListPath")
    val result = FixSic.addValidNonTradingSic(sicList).length
    // plus the 4 valid SICs for non-trading companies (74990, 98000, 99000, 99999)
    val expected = 15596

    result shouldBe expected
  }

  "replaceIncorrectSic" should "be replaced by a valid SIC code" in {
    val sicList = FixSic.createValidSicList(ctxMgr, s"$homeDir/$sicListPath")
    val amendedSicList = FixSic.addValidNonTradingSic(sicList)

    val df =
      Seq(
        "1234a"
      ).toDF("SICCodeSicText_1")

    val results = df.withColumn("SICCodeSicText_1", FixSic.replaceIncorrectSic(amendedSicList)(df("SICCodeSicText_1")))

    val expected = Seq(
      "99999 - 1234a"
    ).toDF("SICCodeSicText_1")

    assertSmallDataFrameEquality(results, expected)
  }

  "replaceIncorrectSic" should "when given a valid sic be replaced based on IDBR methodology" in {
    val sicList = FixSic.createValidSicList(ctxMgr, s"$homeDir/$sicListPath")
    val amendedSicList = FixSic.addValidNonTradingSic(sicList)

    val df =
      Seq(
        "33120 - tester"
      ).toDF("SICCodeSicText_1")

    val results = df.withColumn("SICCodeSicText_1", FixSic.replaceIncorrectSic(amendedSicList)(df("SICCodeSicText_1")))

    val expected = Seq(
      "28302 - tester"
    ).toDF("SICCodeSicText_1")

    assertSmallDataFrameEquality(results, expected)
  }

  "replaceIncorrectSic" should "have a leading zero appended to it and return a non trading company sic(99999)" in {

    val sicList = FixSic.createValidSicList(ctxMgr, s"$homeDir/$sicListPath")
    val amendedSicList = FixSic.addValidNonTradingSic(sicList)

    val df =
      Seq(
        "1234"
      ).toDF("SICCodeSicText_1")

    val results = df.withColumn("SICCodeSicText_1", FixSic.replaceIncorrectSic(amendedSicList)(df("SICCodeSicText_1")))

    val expected = Seq(
      "99999"
    ).toDF("SICCodeSicText_1")

    assertSmallDataFrameEquality(results, expected)
  }

  "replaceIncorrectSic" should "return same input as output" in {

    val sicList = FixSic.createValidSicList(ctxMgr, s"$homeDir/$sicListPath")
    val amendedSicList = FixSic.addValidNonTradingSic(sicList)

    val df =
      Seq(
        "10110 - Hello World"
      ).toDF("SICCodeSicText_1")

    val results = df.withColumn("SICCodeSicText_1", FixSic.replaceIncorrectSic(amendedSicList)(df("SICCodeSicText_1")))

    val expected = Seq(
      "10110 - Hello World"
    ).toDF("SICCodeSicText_1")

    assertSmallDataFrameEquality(results, expected)
  }

}
