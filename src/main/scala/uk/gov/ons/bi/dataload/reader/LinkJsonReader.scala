package uk.gov.ons.bi.dataload.reader

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

/**
  * Created by websc on 08/02/2017.
  */
class LinkJsonReader (srcDir: String, srcFile: String)(implicit val sc: SparkContext) {

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  val src = s"${srcDir}/${srcFile}"

  def readFromJson: DataFrame = {
    sqlContext.read.json(src)
  }

}
