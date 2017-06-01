package uk.gov.ons.bi.dataload.utils

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by websc on 31/03/2017.
  */
class BandMappingsFlatSpec extends FlatSpec with Matchers {

  behavior of "BandMappingsFlatSpec"

  "employmentBand" should "translate all values correctly" in {

    val inputs = List(0,1,2,6,11,21,26,51,76,101,151,201,251,301,501).map(Option(_))
    val outputs : List[Option[String]] = List("A","B","C","D","E","F","G","H","I","J","K","L","M","N","O").map(Option(_))

    val inputsWithNone = inputs ++ None
    val outputsWithNone = outputs ++ None

    val testParams = inputsWithNone.zip(outputsWithNone)

    for {
      (input, expected) <- testParams
    }
      yield (expected should be (BandMappings.employmentBand(input)))
  }


  "turnoverBand" should "translate all values correctly" in {

    val inputs = List(10L, 110L, 510L, 1010L, 2010L, 5010L, 10010L, 40010L, 99999L).map(Option(_))
    val outputs : List[Option[String]] = List("A", "B", "D", "E", "F", "G", "H", "H", "I").map(Option(_))

    val inputsWithNone = inputs ++ None
    val outputsWithNone = outputs ++ None

    val testParams = inputsWithNone.zip(outputsWithNone)

    for {
      (input, expected) <- testParams
    } yield (expected should be (BandMappings.turnoverBand(input)))

  }

  "tradingStatusBand" should "translate all values correctly" in {

    // ONSRBIB-570: invalid code should now translate as None
    val inputs: Seq[Option[String]] = Vector("Active", "Closed", "Dormant", "Insolvent", "INVALID").map(Option(_))
    val outputs = Vector("A", "C", "D", "I").map(Option(_)) ++ None

    // Allow for a None input value as well
    val inputsWithNone = inputs ++ None
    val outputsWithNone = outputs ++ None

    val testParams = inputsWithNone.zip(outputsWithNone)

    for {
      (input, expected) <- testParams
    } yield expected should be (BandMappings.tradingStatusToBand(input))
  }

  "deathCodeTradingStatus" should "translate all values correctly" in {

    // inputs: 0-9, E, M, S, T, plus one bad value, as Options so we can add a None input value
    val inputs: Seq[Option[String]] =  ((0 to 9).toList.map(_.toString) ++ List("E","M", "S", "T", "BAD")).map(Option(_))
    // outputs for each of the input values in same order, as Options to allow for None output
    val outputs: Seq[Option[String]] = List("Active", "Closed", "Active", "Insolvent", "Active",
                                            "Dormant", "Active", "Active", "Active", "Closed",
                                            "Active", "Active", "Active", "Active").map(Option(_)) :+ None

    // Allow for a None input value as well
    val inputsWithNone = inputs ++ None
    val outputsWithNone = outputs ++ None

    val testParams = inputsWithNone.zip(outputsWithNone)

    for {
      (input, expected) <- testParams
    } yield expected shouldBe (BandMappings.deathCodeTradingStatus(input))
  }

}
