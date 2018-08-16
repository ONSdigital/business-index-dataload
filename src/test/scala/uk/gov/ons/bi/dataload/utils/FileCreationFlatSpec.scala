package uk.gov.ons.bi.dataload.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{FlatSpec, Matchers}
import java.io.File

import uk.gov.ons.bi.dataload.ubrn.UbrnManager

/**
  * Created by ChiuA on 15/08/2018.
  */
class FileCreationFlatSpec extends FlatSpec with Matchers {

  "A Links File " should "be read in from a parquet file and return a dataframe" in {

    // setup config
    val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate()

    val homeDir: String = "/Users/ChiuA/projects/dataload/business-index-dataload/src/main/resources"
    val outputFilePath: String = s"$homeDir/LINKS_Output.parquet"
    val inputFilePath: String  = s"$homeDir/legal_units.parquet"

    // Used to create initial input parquet file
    val jsonPath = s"$homeDir/links.json"

    new File(outputFilePath).delete()

    sparkSession.read.json(jsonPath).write.mode("overwrite").parquet(inputFilePath)

    val df: DataFrame = sparkSession.read.parquet(inputFilePath)

    val withNewUbrn = UbrnManager.applyNewUbrn(df)

    withNewUbrn.write.mode("overwrite").parquet(outputFilePath)

    val result = new File(outputFilePath).exists
    result shouldBe true
  }

  "SourceDataToParquetApp"

  "LinkDataApp"

  "LoadBiToEsApp"
}
