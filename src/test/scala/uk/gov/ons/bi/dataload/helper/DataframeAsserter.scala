package uk.gov.ons.bi.dataload.helper

import org.apache.spark.sql.DataFrame

case class DatasetSchemaMismatch(smth: String)  extends Exception(smth)
case class DatasetContentMismatch(smth: String) extends Exception(smth)

trait DataframeAsserter extends DataSetComparer {
  def assertSmallDataFrameEquality(actualDF: DataFrame, expectedDF: DataFrame): Unit = {
    if (!actualDF.schema.equals(expectedDF.schema)) {
      throw new DatasetSchemaMismatch(schemaMismatchMessage(actualDF, expectedDF))
    }
    if (!actualDF.collect().sameElements(expectedDF.collect())) {
      throw new DatasetContentMismatch(basicMismatchMessage(actualDF, expectedDF))
    }
  }
}