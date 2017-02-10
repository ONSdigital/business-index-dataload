package uk.gov.ons.bi.dataload.writer

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

/**
  * Created by websc on 08/02/2017.
  */
class CompaniesHouseWriterParquet(targetDir: String, targetFile: String)(val sc: SparkContext) {

  def write(df: DataFrame) = {

  }

}
