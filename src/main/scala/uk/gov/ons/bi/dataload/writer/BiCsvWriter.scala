package uk.gov.ons.bi.dataload.writer

import org.apache.spark.sql.DataFrame
import uk.gov.ons.bi.dataload.utils.ContextMgr

/**
  * Created by websc on 29/06/2017.
  */

object BiCsvWriter {

  // Write the CSV output files:
  def writeCsvOutput(df: DataFrame, outputFile: String, singleFile: Boolean = true) = {
    // Depends on Spark CSV
    // Spark writes in Hadoop style i.e. creates a directory containing a number of files.
    // Need to push data into one partition to get a single data file in the output directory.
    val outputDf =  if (singleFile) df.repartition(1)
                    else  df

    outputDf.write.mode("overwrite")
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .save(outputFile)

  }
}
