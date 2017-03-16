package uk.gov.ons.bi.dataload.ubrn

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{max, monotonicallyIncreasingId}

import scala.util.{Success, Try}
import com.google.inject.Singleton

/**
  * Created by websc on 16/03/2017.
  */
@Singleton
class UbrnManager(sc: SparkContext) {

  // Use getOrCreate in case SQLContext already exists (only want one)
  val sqlContext: SQLContext = SQLContext.getOrCreate(sc)

  val defaultBaseUbrn = 100000000000L
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

    // Now add the new generated UBRN column and sequence value
    val df1partWithUbrn = df1partition.withColumn(defaultUbrnColName, monotonicallyIncreasingId + base)

    // Repartition back to original num partitions (more data shuffling)
    df1partWithUbrn.repartition(numPartitions)
  }

}
