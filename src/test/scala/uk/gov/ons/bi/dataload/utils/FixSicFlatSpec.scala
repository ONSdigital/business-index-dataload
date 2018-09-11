package uk.gov.ons.bi.dataload.utils

import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}


class FixSicFlatSpec extends FlatSpec with Matchers {

  "A SIC list" should "contain all valid SICs that are possible in the IDBR" in {

    val sicList = FixSic.createValidSicList("/")
    val result = sicList.length
    // Expected is taken from the 15,592 valid SICs published by ONS at
    // plus the 4 valid SICs for non-trading companies (74990, 98000, 99000, 99999)
    val expected = 15596

    result shouldBe expected

  }

  "An incorrect SIC code" should "be replaced by a valid SIC code" in {


  }


  "A 4 digit SIC code" should "have a leading zero appended to it" in {

  }

}
