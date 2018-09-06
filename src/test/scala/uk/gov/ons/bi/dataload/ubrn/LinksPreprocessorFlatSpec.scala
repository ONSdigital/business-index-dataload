package uk.gov.ons.bi.dataload.ubrn

import org.scalatest.{FlatSpec, Matchers}
import uk.gov.ons.bi.dataload.model.BiSparkDataFrames
import uk.gov.ons.bi.dataload.SparkCreator

class LinksPreprocessorFlatSpec extends FlatSpec with Matchers with SparkCreator {

  import spark.implicits._

  val lpp = new LinksPreprocessor(ctxMgr)

  "Links Preprocessor" should "Read set previous Links to empty Dataframe if error" in {
    val emptyDF = lpp.readPrevLinks("invalidDir","invalidFile")

    emptyDF.take(1).isEmpty shouldBe true
  }

  "Links Preprocessor" should "apply UBRN to Dataframe input" in {

    val data = Seq(
      (Array("ch1"), Array(""), Array("065H7Z31732"))
    ).toDF("CH","VAT","PAYE")

    val prevData = BiSparkDataFrames.emptyLinkWithUbrnDf(ctxMgr)

    val actual = lpp.preProcessLinks(data, prevData)

    val expected = Seq(
      (1000000000000001L, Array("ch1"), Array(""), Array("065H7Z31732"))
    ).toDF("UBRN","CH","VAT","PAYE")

    actual.collect() shouldBe expected.collect()
  }

  "Links Preprocessor" should "apply UBRN to newly created Legal Units and Edit existing links" in {

    val data = Seq(
      (Array("ch1"), Array(""), Array("")),
      (Array("ch3"), Array(""), Array("paye1")),
      (Array("ch2"), Array("vat1"), Array("")),
      (Array(""), Array(""), Array("paye2"))
    ).toDF("CH","VAT","PAYE")

    val prevData = Seq(
      (1000000000000001L, Array("ch1"), Array(""), Array("paye1")),
      (1000000000000010L, Array("ch2"), Array(""), Array("")),
      (1000000000000020L, Array("ch3"), Array(""), Array("paye2"))
    ).toDF("UBRN", "CH","VAT","PAYE")

    val actual = lpp.preProcessLinks(data, prevData)

    val expected = Seq(
      (1000000000000001L, Array("ch1"), Array(""), Array("")),
      (1000000000000010L, Array("ch2"), Array("vat1"), Array("")),
      (1000000000000020L, Array("ch3"), Array(""), Array("paye1")),
      (1000000000000021L, Array(""), Array(""), Array("paye2"))
    ).toDF("UBRN","CH","VAT","PAYE")

    actual.collect() shouldBe expected.collect()
  }

}