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

    val inputs: Seq[Option[String]] = Vector("Active", "Closed", "Dormant", "Insolvent", "INVALID").map(Option(_))
    val outputs = Vector("A", "C", "D", "I", "?").map(Option(_))

    val inputsWithNone = inputs ++ None
    val outputsWithNone = outputs ++ None

    val testParams = inputsWithNone.zip(outputsWithNone)

    for {
      (input, expected) <- testParams
    } yield expected should be (BandMappings.tradingStatusBand(input))
  }

}
