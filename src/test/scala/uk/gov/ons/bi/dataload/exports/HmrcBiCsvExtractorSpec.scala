package uk.gov.ons.bi.dataload.exports

import org.apache.spark.sql.{AnalysisException, Row}
import org.scalatest.{FlatSpec, Matchers}
import uk.gov.ons.bi.dataload.SparkSessionSpec
import uk.gov.ons.bi.dataload.model.TestModel

class HmrcBiCsvExtractorSpec extends FlatSpec with SparkSessionSpec with Matchers{

  "getLegalEntities" should "return the correct columns as a dataframe" in {
    val hmrc = Seq(
      Row(1000000000000002L, "! LTD", "tradstyle1", 1000000000000002L, "LS10 2RU", "METROHOUSE 57 PEPPER ROAD", "HUNSLET", "LEEDS", "YORKSHIRE", null, "99999", "1", "A", "A", null, "08209948", Array(312764963000L), Array())
    )
    val hmrcFrame = spark.createDataFrame(sc.parallelize(hmrc), TestModel.hmrcSchema)

    val expected = Array("id",
      "BusinessName",
      "TradingStyle",
      "PostCode",
      "Address1",
      "Address2",
      "Address3",
      "Address4",
      "Address5",
      "IndustryCode",
      "LegalStatus",
      "TradingStatus",
      "Turnover",
      "EmploymentBands",
      "CompanyNo")

    val result = HmrcBiCsvExtractor.getLegalEntities(hmrcFrame).columns
    assert ( result sameElements expected )
  }

  it should "throw AnalysisException if an invalid schema is used" in {
    val invalid = Seq(
      Row(1000000000000002L, null, null, null, null, null, null)
    )
    val invalidFrame = spark.createDataFrame(sc.parallelize(invalid), TestModel.invalidSchema)

    assertThrows[AnalysisException] {
      HmrcBiCsvExtractor.getLegalEntities(invalidFrame).columns
    }
  }
}
