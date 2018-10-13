package uk.gov.ons.bi.dataload.ubrn

import java.util.UUID

import org.scalatest.{FlatSpec, Matchers}
import uk.gov.ons.bi.dataload.model.BiSparkDataFrames
import uk.gov.ons.bi.dataload.SparkSessionSpec
import uk.gov.ons.bi.dataload.helper.DataframeAsserter
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import uk.gov.ons.bi.dataload.PreprocessLinksApp.appConfig
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

class LinksPreprocessorFlatSpec extends FlatSpec with Matchers with SparkSessionSpec with DataframeAsserter {

  import spark.implicits._

  val lpp = new LinksPreprocessor(ctxMgr)

  // getAdminFilePaths
  val externalDir = appConfig.External.externalPath
  val vatPath = s"$externalDir/${appConfig.External.vatPath}"
  val payePath = s"$externalDir/${appConfig.External.payePath}"

  /*
  "readPrevLinks" should "return an empty DataFrame when given invalid inputs" in {

      val emptyDF = lpp.readPrevLinks("invalidDir", "invalidFile")

      val firstRow: Int = 1
      emptyDF.take(firstRow).isEmpty shouldBe true
  }

  "preProcessLinks" should "apply UBRN to newly created Legal Units where previous links is empty and new links are valid" in {

    val newLinks = Seq(
      (Array("ch1"), Array(""), Array("065H7Z31732"))
    ).toDF("CH","VAT","PAYE")

    val prevLinks = BiSparkDataFrames.emptyLinkWithUbrnDf(ctxMgr)

    val actual = lpp.preProcessLinks(newLinks, prevLinks, vatPath, payePath)

    val expected = Seq(
      Row(1000000000000001L, Array("ch1"), Array(""), Array("065H7Z31732"))
    )
    val expectedDf = spark.createDataFrame(sc.parallelize(expected),BiSparkDataFrames.linkWithUbrnSchema)

    assertSmallDataFrameEquality(actual, expectedDf)
  }

  it should "apply UBRN to newly created Legal Units and Edit existing links given valid inputs for new links and previous links" in {

    val newLinks = Seq(
      (Array("ch1"), Array(""), Array("")),
      (Array("ch3"), Array(""), Array("paye1")),
      (Array("ch2"), Array("vat1"), Array("")),
      (Array(""), Array(""), Array("paye2"))
    ).toDF("CH","VAT","PAYE")

    val prevLinks = Seq(
      (1000000000000001L, Array("ch1"), Array(""), Array("paye1")),
      (1000000000000010L, Array("ch2"), Array(""), Array("")),
      (1000000000000020L, Array("ch3"), Array(""), Array("paye2"))
    ).toDF("UBRN", "CH","VAT","PAYE")

    val actual = lpp.preProcessLinks(newLinks, prevLinks, vatPath, payePath)

    val expected = Seq(
      Row(1000000000000001L, Array("ch1"), Array(""), Array("")),
      Row(1000000000000010L, Array("ch2"), Array("vat1"), Array("")),
      Row(1000000000000020L, Array("ch3"), Array(""), Array("paye1")),
      Row(1000000000000021L, Array(""), Array(""), Array("paye2"))
    )
    val expectedDf = spark.createDataFrame(sc.parallelize(expected),BiSparkDataFrames.linkWithUbrnSchema)

    assertSmallDataFrameEquality(actual, expectedDf)
  }

  it should "Update UBRN with dead vat" in {

    val newLinks = Seq(
      (Array(""), Array("vat1","vat3"), Array(""))
    ).toDF("CH","VAT","PAYE")

    val prevLinks = Seq(
      (1000000000000100L, Array(""), Array("vat1", "vat2","vat3"), Array(""))
    ).toDF("UBRN", "CH","VAT","PAYE")

    val actual = lpp.preProcessLinks(newLinks, prevLinks, vatPath, payePath)

    val expected = Seq(
      Row(1000000000000100L, Array(""), Array("vat1", "vat3"), Array(""))
    )

    val expectedDf = spark.createDataFrame(sc.parallelize(expected),BiSparkDataFrames.linkWithUbrnSchema)

    assertSmallDataFrameEquality(actual, expectedDf)
  }
  
  it should "IllegalArgumentException error when applying UBRN to invalid inputs for newlinks and previous links" in {

    val newLinks = BiSparkDataFrames.emptyLinkWithUbrnDf(ctxMgr)
    val prevLinks = BiSparkDataFrames.emptyLinkWithUbrnDf(ctxMgr)

    assertThrows[IllegalArgumentException]{
      lpp.preProcessLinks(newLinks, prevLinks, vatPath, payePath)
    }
  }

  it should "example 1 - merge admin units into one legal unit" in {

    val newLinks = Seq(
      (Array(""), Array("VAT1"), Array("PAYE2")),
      (Array(""), Array(""), Array(""))
    ).toDF("CH","VAT","PAYE")

    val prevLinks = Seq(
      (1000000000000100L, Array(""), Array("VAT1"), Array("")),
      (1000000000000101L, Array(""), Array(""), Array("PAYE2"))
    ).toDF("UBRN", "CH","VAT","PAYE")

    val actual = lpp.preProcessLinks(newLinks, prevLinks, vatPath, payePath)

    val expected = Seq(
      Row(1000000000000100L, Array(""), Array("VAT1"), Array("PAYE2")),
      Row(1000000000000101L, Array(""), Array(""), Array(""))
    )

    val expectedDf = spark.createDataFrame(sc.parallelize(expected),BiSparkDataFrames.linkWithUbrnSchema)

    assertSmallDataFrameEquality(actual, expectedDf)
  }

  it should "example 2 - split admin units into multipe legal units" in {

    val newLinks = Seq(
      (Array(""), Array("VAT1"), Array("")),
      (Array(""), Array(""), Array("PAYE1"))
    ).toDF("CH","VAT","PAYE")

    val prevLinks = Seq(
      (1000000000000100L, Array(""), Array("VAT1"), Array("PAYE1"))
    ).toDF("UBRN", "CH","VAT","PAYE")

    val actual = lpp.preProcessLinks(newLinks, prevLinks, vatPath, payePath)

    val expected = Seq(
      Row(1000000000000100L, Array(""), Array("VAT1"), Array("")),
      Row(1000000000000101L, Array(""), Array(""), Array("PAYE1"))
    )

    val expectedDf = spark.createDataFrame(sc.parallelize(expected),BiSparkDataFrames.linkWithUbrnSchema)

    assertSmallDataFrameEquality(actual, expectedDf)
  }

  it should "example 3 - move admin units between multiple legal units" in {

    val newLinks = Seq(
      (Array(""), Array("VAT1"), Array("PAYE1")),
      (Array(""), Array("VAT2"), Array("PAYE2"))
    ).toDF("CH","VAT","PAYE")

    val prevLinks = Seq(
      (1000000000000100L, Array(""), Array("VAT1"), Array("PAYE1","PAYE2")),
      (1000000000000101L, Array(""), Array("VAT2"), Array(""))
    ).toDF("UBRN", "CH","VAT","PAYE")

    val actual = lpp.preProcessLinks(newLinks, prevLinks, vatPath, payePath)

    val expected = Seq(
      Row(1000000000000100L, Array(""), Array("VAT1"), Array("PAYE1")),
      Row(1000000000000101L, Array(""), Array("VAT2"), Array("PAYE2"))
    )

    val expectedDf = spark.createDataFrame(sc.parallelize(expected),BiSparkDataFrames.linkWithUbrnSchema)

    assertSmallDataFrameEquality(actual, expectedDf)
  }

  it should "example 4 - Splits and appends admin units across multiple legal units" in {

    val newLinks = Seq(
      (Array("CH1"), Array("VAT1"), Array("PAYE1","PAYE1000")),
      (Array("CH2"), Array("VAT2"), Array("PAYE2")),
      (Array(""), Array("VAT3"), Array("PAYE3"))
    ).toDF("CH","VAT","PAYE")

    val prevLinks = Seq(
      (1000000000000100L, Array("CH1"), Array("VAT1", "VAT2", "VAT3"), Array("PAYE1", "PAYE2", "PAYE3")),
      (1000000000000101L, Array("CH2"), Array("VAT2"), Array(""))
    ).toDF("UBRN", "CH","VAT","PAYE")

    val actual = lpp.preProcessLinks(newLinks, prevLinks, vatPath, payePath)

    val expected = Seq(
      Row(1000000000000100L, Array("CH1"), Array("VAT1"), Array("PAYE1", "PAYE1000")),
      Row(1000000000000101L, Array("CH2"), Array("VAT2"), Array("PAYE2")),
      Row(1000000000000102L, Array(""), Array("VAT3"), Array("PAYE3"))
    )

    val expectedDf = spark.createDataFrame(sc.parallelize(expected),BiSparkDataFrames.linkWithUbrnSchema)

    assertSmallDataFrameEquality(actual, expectedDf)
  }

  it should "example 5 - No UBRN value present in legal unit structure so new UBRN is applied" in {

    val newLinks = Seq(
      (Array("CH2"), Array("VAT2"), Array("PAYE2")),
      (Array(""), Array("VAT7"), Array("PAYE7"))
    ).toDF("CH","VAT","PAYE")

    val prevLinks = Seq(
      (1000000000000101L, Array("CH2"), Array("VAT2"), Array("")),
      (1000000000000102L, Array(""), Array("VAT7"), Array("")),
      (1000000000000103L, Array(""), Array(""), Array("PAYE7"))
    ).toDF("UBRN", "CH","VAT","PAYE")

    val actual = lpp.preProcessLinks(newLinks, prevLinks, vatPath, payePath)

    val expected = Seq(
      Row(1000000000000101L, Array("CH2"), Array("VAT2"), Array("PAYE2")),
      Row(1000000000000104L, Array(""), Array("VAT7"), Array("PAYE7"))
    )

    val expectedDf = spark.createDataFrame(sc.parallelize(expected),BiSparkDataFrames.linkWithUbrnSchema)

    assertSmallDataFrameEquality(actual, expectedDf)

  }
  */

  it should "test bed" in {
    val newLinks = Seq(
      (Array("1"), Array("868500288000", "123764963000"), Array("035H7A22627", "125H7A71620")),
      (Array("2"), Array("868504062000"), Array("065H7Z31732")),
      (Array("CH3"), Array(""), Array(""))
    ).toDF("CH","VAT","PAYE")

    val prevLinks = Seq(
      (1000000000000101L, Array(""), Array("868500288000", "312764963000"), Array("")),
      (1000000000000102L, Array(""), Array("868504062000"), Array("065H7Z31732", "035H7A22627")),
      (1000000000000103L, Array("CH3"), Array(""), Array(""))
    ).toDF("UBRN", "CH","VAT","PAYE")

    val actual = lpp.preProcessLinks(newLinks, prevLinks, vatPath, payePath)

    actual.show(false)

    val expected = Seq(
      Row(1000000000000101L, Array("CH2"), Array("VAT2"), Array("PAYE2")),
      Row(1000000000000104L, Array(""), Array("VAT7"), Array("PAYE7"))
    )

    val expectedDf = spark.createDataFrame(sc.parallelize(expected),BiSparkDataFrames.linkWithUbrnSchema)

    assertSmallDataFrameEquality(actual, expectedDf)
  }
}