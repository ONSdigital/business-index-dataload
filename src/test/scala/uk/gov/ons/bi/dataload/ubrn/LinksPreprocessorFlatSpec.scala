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

  "readPrevLinks" should "return an empty DataFrame when given invalid inputs" in {

      val emptyDF = lpp.readPrevLinks("invalidDir", "invalidFile")

      val firstRow: Int = 1
      emptyDF.take(firstRow).isEmpty shouldBe true
  }

  "preProcessLinks" should "apply UBRN to newly created Legal Units where previous links is empty and new links are valid" in {

    val newLinks = Seq(
      (Array("ch1"), Array(null.asInstanceOf[String]), Array("065H7Z31732"))
    ).toDF("CH","VAT","PAYE")

    val prevLinks = BiSparkDataFrames.emptyLinkWithUbrnDf(ctxMgr)

    val actual = lpp.preProcessLinks(newLinks, prevLinks, vatPath, payePath)

    val expected = Seq(
      Row(1000000000000001L, Array("ch1"), Array(null.asInstanceOf[String]), Array("065H7Z31732"))
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

  it should "Split and appends admin units across multiple legal units" in {

    val newLinks = Seq(
      (Array("CH1"), Array("862764963000"), Array("065H7Z31732","065H7Z31732000")),
      (Array("CH2"), Array("123764963000"), Array("035H7A22627")),
      (Array(null.asInstanceOf[String]), Array("312764963000"), Array("125H7A71620"))
    ).toDF("CH","VAT","PAYE")

    val prevLinks = Seq(
      (1000000000000100L, Array("CH1"), Array("862764963000", "123764963000", "312764963000"), Array("065H7Z31732", "035H7A22627", "125H7A71620")),
      (1000000000000101L, Array("CH2"), Array("123764963000"), Array(null.asInstanceOf[String]))
    ).toDF("UBRN", "CH","VAT","PAYE")

    val actual = lpp.preProcessLinks(newLinks, prevLinks, vatPath, payePath)

    val expected = Seq(
      Row(1000000000000100L, Array("CH1"), Array("862764963000"), Array("065H7Z31732", "065H7Z31732000")),
      Row(1000000000000101L, Array("CH2"), Array("123764963000"), Array("035H7A22627")),
      Row(1000000000000102L, Array(null.asInstanceOf[String]), Array("312764963000"), Array("125H7A71620"))
    )

    val expectedDf = spark.createDataFrame(sc.parallelize(expected),BiSparkDataFrames.linkWithUbrnSchema)

    assertSmallDataFrameEquality(actual, expectedDf)
  }

  it should "Generate a new UBRN, attach to an old non CH unit and a CH unit" in {
    val newLinks = Seq(
      (Array(null.asInstanceOf[String]), Array("868500288000", "123764963000"), Array("035H7A22627", "125H7A71620")),
      (Array(null.asInstanceOf[String]), Array("868504062000"), Array("065H7Z31732")),
      (Array("CH3"), Array(null.asInstanceOf[String]), Array(null.asInstanceOf[String]))
    ).toDF("CH","VAT","PAYE")

    val prevLinks = Seq(
      (1000000000000101L, Array(null.asInstanceOf[String]), Array("868500288000", "312764963000"), Array(null.asInstanceOf[String])),
      (1000000000000102L, Array(null.asInstanceOf[String]), Array("868504062000"), Array("065H7Z31732", "035H7A22627")),
      (1000000000000103L, Array("CH3"), Array(null.asInstanceOf[String]), Array(null.asInstanceOf[String]))
    ).toDF("UBRN", "CH","VAT","PAYE")

    val actual = lpp.preProcessLinks(newLinks, prevLinks, vatPath, payePath).sort("UBRN")

    val expected = Seq(
      Row(1000000000000102L, Array(null.asInstanceOf[String]), Array("868500288000", "123764963000"), Array("035H7A22627", "125H7A71620")),
      Row(1000000000000103L, Array("CH3"), Array(null.asInstanceOf[String]), Array(null.asInstanceOf[String])),
      Row(1000000000000104L, Array(null.asInstanceOf[String]),Array("868504062000"), Array("065H7Z31732"))
    )

    val expectedDf = spark.createDataFrame(sc.parallelize(expected),BiSparkDataFrames.linkWithUbrnSchema).sort("UBRN")

    assertSmallDataFrameEquality(actual, expectedDf)
  }
}