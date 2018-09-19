package uk.gov.ons.bi.dataload.ubrn

import org.scalatest.{FlatSpec, Matchers}

import uk.gov.ons.bi.dataload.model.BiSparkDataFrames
import uk.gov.ons.bi.dataload.SparkSessionSpec
import uk.gov.ons.bi.dataload.helper.DataframeAsserter
import org.apache.spark.sql.Row

class LinksPreprocessorFlatSpec extends FlatSpec with Matchers with SparkSessionSpec with DataframeAsserter {

  import spark.implicits._

  val lpp = new LinksPreprocessor(ctxMgr)

  "readPrevLinks" should "return an empty DataFrame when given invalid inputs" in {
      val emptyDF = lpp.readPrevLinks("invalidDir","invalidFile")

      val firstRow: Int = 1
      emptyDF.take(firstRow).isEmpty shouldBe true
  }

  "preProcessLinks" should "apply UBRN to newly created Legal Units where previous links is empty and new links are valid" in {

    val newLinks = Seq(
      (Array("ch1"), Array(""), Array("065H7Z31732"))
    ).toDF("CH","VAT","PAYE")

    val prevLinks = BiSparkDataFrames.emptyLinkWithUbrnDf(ctxMgr)

    val actual = lpp.preProcessLinks(newLinks, prevLinks)

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

    val actual = lpp.preProcessLinks(newLinks, prevLinks)

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

    val actual = lpp.preProcessLinks(newLinks, prevLinks)

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
      lpp.preProcessLinks(newLinks,prevLinks)
    }
  }

}