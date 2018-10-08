package uk.gov.ons.bi.dataload.ubrn

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.util.{Success, Try}

object UbrnManager {

  val defaultBaseUbrn = 1000000000000000L
  val defaultUbrnColName = "UBRN"

  def getMaxUbrn(df: DataFrame, ubrnColName: String = defaultUbrnColName): Option[Long] = {
    // This will scan the DF column to extract the max value, assumes values are numeric.
    // Defaults to zero.
    Try {
      val row = df.agg(max(df(ubrnColName))).collect.head
      row.getLong(0)
    }
    match {
      case Success(n: Long) => Some(n)
      case _ => Some(defaultBaseUbrn)
    }
  }

  def applyNewUbrn(df: DataFrame, baseUbrn: Option[Long] = None): DataFrame = {
    // First drop any rogue UBRN column (if any) from the input DF
    val noUbrn = df.drop(defaultUbrnColName)

    // Set the base UBRN for adding to the monotonic sequential value
    val base = baseUbrn.getOrElse(defaultBaseUbrn) + 1

    // Repartition to one partition so sequence is a fairly continuous range.
    // This will force data to be shuffled, which is inefficient.
    val numPartitions = df.rdd.getNumPartitions

    val df1partition = df.repartition(1)

    val test1 = monotonically_increasing_id() + base
    val test2 = when(col("GID").isNull ,null).otherwise(monotonically_increasing_id() + base)

    // Now add the new generated UBRN column and sequence value
    val df1partWithUbrn = df1partition.withColumn(defaultUbrnColName, when(col("GID").isNull ,null).otherwise(monotonically_increasing_id() + base))

    // Repartition back to original num partitions (more data shuffling)
    df1partWithUbrn.repartition(numPartitions)
  }

}
