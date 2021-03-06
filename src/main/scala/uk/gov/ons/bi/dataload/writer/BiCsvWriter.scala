package uk.gov.ons.bi.dataload.writer

import org.apache.spark.sql.DataFrame

object BiCsvWriter {

  def writeCsvOutput(df: DataFrame, outputFile: String, singleFile: Boolean = true) = {
    // Depends on Spark CSV
    // Spark writes in Hadoop style i.e. creates a directory containing a number of files.
    // Need to push data into one partition to get a single data file in the output directory.
    // Switched to coalesce here from repartition to do the same thing without a full shuffle of the data since only using one partition
    val outputDf =  if (singleFile) df.coalesce(1)
                    else  df

    outputDf.write.mode("overwrite")
        .option("header", "true")
        .option("quoteAll", "true")
        .option("nullValue","")
        .csv(outputFile)

  }


}
