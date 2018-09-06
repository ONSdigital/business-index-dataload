package uk.gov.ons.bi.dataload.helper

import org.apache.spark.sql.{DataFrame, Dataset}

trait DataSetComparer {

  def schemaMismatchMessage[T](actualDS: Dataset[T], expectedDS: Dataset[T]): String = {
    s"""
      Actual Schema:
      ${actualDS.schema}
      Expected Schema:
      ${expectedDS.schema}
      """
  }

  def basicMismatchMessage[T](actualDf: DataFrame, expectedDf: DataFrame): String = {
    s"""
      Actual DataFrame Content:
      ${actualDf.collect().toSeq}
      Expected DataFrame Content:
      ${expectedDf.collect().toSeq}
      """
  }
}