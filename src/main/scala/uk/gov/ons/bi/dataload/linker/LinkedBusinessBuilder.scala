package uk.gov.ons.bi.dataload.linker
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import uk.gov.ons.bi.dataload.model._
import uk.gov.ons.bi.dataload.reader.ParquetReader
import uk.gov.ons.bi.dataload.utils.{AppConfig, Transformers}


/**
  * Created by websc on 16/02/2017.
  */
object LinkedBusinessBuilder {
  // NOTE:
  // This needs to be an object, not a Singleton, because we get weird Spark "Task not serializable"
  // errors when there is a lot of nested RDD processing around here. Might be better in Spark 2.x?


  // This object contains Spark-specific code for processing RDDs and DataFrames.
  // Non-Spark transformations are in the separate Transformers object.

  def convertUwdsToBusinessRecords(uwds: RDD[UbrnWithData]): RDD[Business] = {
    // Now we can group data for same UBRN back together
    val grouped: RDD[(String, Iterable[UbrnWithData])] = uwds.map { r => (r.ubrn, r) }.groupByKey()
    val uwls: RDD[UbrnWithList] = grouped.map { case (ubrn, uwds) => UbrnWithList(ubrn, uwds.toList) }
    // Convert each UBRN group to a Business record
    uwls.map(Transformers.buildBusinessRecord)
  }

  def writeBiRddToParquet(sc: SparkContext, appConfig: AppConfig, biRdd: RDD[BusinessIndex]) = {
    // Need some voodoo here to convert RDD[BusinessIndex] back to DataFrame.
    // This effectively defines the format of the final BI record in ElasticSearch.

    val biSchema = StructType(Seq(
      StructField("id", StringType, true), // not clear where this comes from.  use UBRN for now
      StructField("businessName", StringType, true),
      StructField("uprn", StringType, true), // spec says "UPRN", but we use UBRN
      StructField("postCode", StringType, true),
      StructField("industryCode", StringType, true),
      StructField("legalStatus", StringType, true),
      StructField("tradingStatus", StringType, true),
      StructField("turnover", StringType, true),
      StructField("employmentBand", StringType, true)
    ))

    // Use UBRN as ID and UPRN in index until we have better information
    def biRowMapper(bi: BusinessIndex): Row = {
      Row(bi.ubrn, bi.businessName, bi.ubrn, bi.postCode, bi.industryCode, bi.legalStatus, bi.tradingStatus,
        bi.turnoverBand, bi.employmentBand)
    }

    val biRows: RDD[Row] = biRdd.map(biRowMapper)

    val sqc = new SQLContext(sc)

    val biDf: DataFrame = sqc.createDataFrame(biRows, biSchema)

    // Write BI DataFrame to Parquet file. We will load it into ElasticSearch separately.

    val parquetDataConfig = appConfig.ParquetDataConfig
    val parquetPath = parquetDataConfig.dir
    val parquetBiFile = parquetDataConfig.bi
    val biFile = s"$parquetPath/$parquetBiFile"

    biDf.write.mode("overwrite").parquet(biFile)
  }

  // ***************** Link UBRN to Company/VAT/PAYE data **************************

  def getLinkedCompanyData(uwks: RDD[UbrnWithKey], pqReader: ParquetReader,
                           appConfig: AppConfig, sc: SparkContext): RDD[UbrnWithData] = {

    // Company/VAT/PAYE: format data as (key, data) pairs so we can use RDD joins below

    val cos: RDD[(String, CompanyRec)] = pqReader.loadCompanyRecsFromParquet(appConfig)

    // Join Links to corresponding data

    val linkedData: RDD[UbrnWithData] = uwks.filter { r => r.src == CH }.map { r => (r.key, r) }
      .join(cos)
      .map { case (key, (uwk, data))
      => UbrnWithData(uwk.ubrn, uwk.src, data)
      }

    linkedData
  }

  def getLinkedVatData(uwks: RDD[UbrnWithKey], pqReader: ParquetReader,
                       appConfig: AppConfig, sc: SparkContext): RDD[UbrnWithData] = {

    // Company/VAT/PAYE: format data as (key, data) pairs so we can use RDD joins below

    val vats: RDD[(String, VatRec)] = pqReader.loadVatRecsFromParquet(appConfig)

    // Join Links to corresponding data

    val linkedData: RDD[UbrnWithData] = uwks.filter { r => r.src == VAT }.map { r => (r.key, r) }
      .join(vats)
      .map { case (key, (uwk, data))
      => UbrnWithData(uwk.ubrn, uwk.src, data)
      }

    linkedData
  }

  def getLinkedPayeData(uwks: RDD[UbrnWithKey], pqReader: ParquetReader,
                        appConfig: AppConfig, sc: SparkContext): RDD[UbrnWithData] = {

    // Company/VAT/PAYE: format data as (key, data) pairs so we can use RDD joins below

    val payes: RDD[(String, PayeRec)] = pqReader.loadPayeRecsFromParquet(appConfig)

    // Join Links to corresponding data

    val linkedData: RDD[UbrnWithData] = uwks.filter { r => r.src == PAYE }.map { r => (r.key, r) }
      .join(payes)
      .map { case (key, (uwk, data))
      => UbrnWithData(uwk.ubrn, uwk.src, data)
      }

    linkedData
  }


  // ***************** MAIN BI linking process below **************************

  def buildLinkedBusinessIndexRecords(sc: SparkContext, appConfig: AppConfig) = {

    // Load Parquet source data and links
    val pqReader = new ParquetReader(sc)

    val links: RDD[LinkRec] = pqReader.loadLinkRecsFromParquet(appConfig)

    // explodeLink() converts each nested Link record to a sequence of (UBRN, type, key) triples.
    // flatMap(identity) then turns it from an RDD[Seq[UbrnWithKey]] into an
    // RDD[UbrnWithKey], which is what we want.

    val uwks: RDD[UbrnWithKey] = links.map { ln => Transformers.explodeLink(ln) }.flatMap(identity)

    // Cache this data as we will be doing different things to it below
    uwks.cache()

    // Join Links to corresponding company/VAT/PAYE data

    val companyData: RDD[UbrnWithData] = getLinkedCompanyData(uwks, pqReader, appConfig, sc)

    val vatData: RDD[UbrnWithData] = getLinkedVatData(uwks, pqReader, appConfig, sc)

    val payeData: RDD[UbrnWithData] = getLinkedPayeData(uwks, pqReader, appConfig, sc)

    // Put the lists of UWDs  (UBRN, src, data) back together
    val combined: RDD[UbrnWithData] = companyData ++ vatData ++ payeData

    // Now we can group data for same UBRN back together to make Business records
    val businessRecords = convertUwdsToBusinessRecords(combined)

    // Now we can convert Business records to Business Index entries

    val businessIndexes = businessRecords.map(Transformers.convertToBusinessIndex)

    // write BI data to parquet file
    writeBiRddToParquet(sc, appConfig, businessIndexes)

    // clear cached UWKs
    uwks.unpersist()
  }

}
