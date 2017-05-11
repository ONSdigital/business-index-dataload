package uk.gov.ons.bi.dataload.ubrn

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by websc on 31/03/2017.
  */
class UbrnManagerFlatSpec extends FlatSpec with SharedSparkContext with Matchers {

  behavior of "UbrnManagerFLatSpec"

  "UbrnManager" should "getMaxUbrn correctly from list of UBRNs" in {
    val defaultBaseUbrn = 100000000000L
    val defaultUbrnColName = "UBRN"
    // Need SQLContext implicits for RDD->DF conversion
    val sqlCtx = new SQLContext(sc)
    import sqlCtx.implicits._


    // Build a dataframe of UBRNs (don't need other data)
    val ubrns = sc.parallelize(Seq(5L, 4L, 2L, 7L, 9L, 1L))
    val ubrnsDf = ubrns.toDF(defaultUbrnColName)

    val expected = Some(9L)

    val results = UbrnManager.getMaxUbrn(ubrnsDf, defaultUbrnColName)

    results should be (expected)
  }

  "UbrnManager" should "getMaxUbrn correctly from EMPTY list of UBRNs" in {
    val defaultBaseUbrn = 100000000000L
    val defaultUbrnColName = "UBRN"
    // Need SQLContext implicits for RDD->DF conversion
    val sqlCtx = new SQLContext(sc)
    //import sqlCtx.implicits._

    // UBRN DataFrame schema
    val ubrnSchema = StructType(Seq(
      StructField(defaultUbrnColName, LongType, true)
    ))
    // Now make an empty UBRN DF
    val emptyUbrnDf:DataFrame  =
      sqlCtx.createDataFrame(sc.emptyRDD[Row], ubrnSchema)


    val expected = Some(defaultBaseUbrn)
    val results = UbrnManager.getMaxUbrn(emptyUbrnDf, defaultUbrnColName)

    results should be (expected)
  }

  "UbrnManager" should "applyNewUbrn correctly to a list of UBRNs" in {
    val defaultBaseUbrn = 100000000000L
    val defaultUbrnColName = "UBRN"
    // Need SQLContext implicits for RDD->DF conversion
    val sqlCtx = new SQLContext(sc)
    import sqlCtx.implicits._

    // Build a dataframe of UBRNs (don't need other data)
    val ubrns = (1 to 9).map(_.toLong)
    val ubrnsDf = sc.parallelize(ubrns).toDF(defaultUbrnColName)

    // Run applyNewUbrn which drops existing UBRNs and creates new ones counting from given base UBRN
    val results = UbrnManager.applyNewUbrn(ubrnsDf,Some(defaultBaseUbrn)).map{row => row(0)}.collect()

    val expected = ubrns.map(_ + defaultBaseUbrn).toArray

    results should contain theSameElementsAs expected
  }


}