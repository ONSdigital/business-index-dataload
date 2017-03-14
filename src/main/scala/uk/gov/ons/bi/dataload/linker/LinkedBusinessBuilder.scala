package uk.gov.ons.bi.dataload.linker

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import uk.gov.ons.bi.dataload.model._
import uk.gov.ons.bi.dataload.reader._
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
    val grouped: RDD[(BiTypes.Ubrn, Iterable[UbrnWithData])] = uwds.map { r => (r.ubrn, r) }.groupByKey()
    val uwls: RDD[UbrnWithList] = grouped.map { case (ubrn, uwds) => UbrnWithList(ubrn, uwds.toList) }
    // Convert each UBRN group to a Business record
    uwls.map(Transformers.buildBusinessRecord)
  }

  def writeBiRddToParquet(sc: SparkContext, appConfig: AppConfig, biRdd: RDD[BusinessIndex]) = {
    // Need some voodoo here to convert RDD[BusinessIndex] back to DataFrame.
    // This effectively defines the format of the final BI record in ElasticSearch.

    val biSchema = StructType(Seq(
      StructField("id", LongType, true), // not clear where this comes from.  use UBRN for now
      StructField("BusinessName", StringType, true),
      StructField("UPRN", LongType, true), // spec says "UPRN", but we use UBRN
      StructField("PostCode", StringType, true),
      StructField("IndustryCode", LongType, true),
      StructField("LegalStatus", StringType, true),
      StructField("TradingStatus", StringType, true),
      StructField("Turnover", StringType, true),
      StructField("EmploymentBands", StringType, true),
      StructField("CompanyNo", StringType, true),
      StructField("VatRefs", ArrayType(LongType), true), // sequence of Long VAT refs
      StructField("PayeRefs", ArrayType(StringType), true) // seq of String PAYE refs
    ))

    // Use UBRN as ID and UPRN in index until we have better information
    def biRowMapper(bi: BusinessIndex): Row = {
      Row(bi.ubrn, bi.businessName, bi.ubrn, bi.postCode, bi.industryCode, bi.legalStatus,
        bi.tradingStatus, bi.turnoverBand, bi.employmentBand, bi.companyNo, bi.vatRefs, bi.payeRefs)
    }

    val biRows: RDD[Row] = biRdd.map(biRowMapper)

    val sqc = new SQLContext(sc)

    val biDf: DataFrame = sqc.createDataFrame(biRows, biSchema)

    // Write BI DataFrame to Parquet file. We will load it into ElasticSearch separately.

    val appDataConfig = appConfig.AppDataConfig
    val workDir = appDataConfig.workingDir
    val parquetBiFile = appDataConfig.bi
    val biFile = s"$workDir/$parquetBiFile"

    biDf.write.mode("overwrite").parquet(biFile)
  }

  // ***************** Link UBRN to Company/VAT/PAYE data **************************

  def getLinkedCompanyData(uwks: RDD[UbrnWithKey],
                           appConfig: AppConfig, sc: SparkContext): RDD[UbrnWithData] = {

    // Company/VAT/PAYE: format data as (key, data) pairs so we can use RDD joins below

    val pqReader = new CompanyRecsParquetReader(sc)
    val cos: RDD[(String, CompanyRec)] = pqReader.loadFromParquet(appConfig)

    // Join Links to corresponding data

    val linkedData: RDD[UbrnWithData] = uwks.filter { r => r.src == CH }.map { r => (r.key, r) }
      .join(cos)
      .map { case (key, (uwk, data))
      => UbrnWithData(uwk.ubrn, uwk.src, data)
      }

    linkedData
  }

  def getLinkedVatData(uwks: RDD[UbrnWithKey],
                       appConfig: AppConfig, sc: SparkContext): RDD[UbrnWithData] = {

    // Company/VAT/PAYE: format data as (key, data) pairs so we can use RDD joins below

    val pqReader = new VatRecsParquetReader(sc)

    val vats: RDD[(String, VatRec)] = pqReader.loadFromParquet(appConfig)

    // Join Links to corresponding data

    val linkedData: RDD[UbrnWithData] = uwks.filter { r => r.src == VAT }.map { r => (r.key, r) }
      .join(vats)
      .map { case (key, (uwk, data))
      => UbrnWithData(uwk.ubrn, uwk.src, data)
      }

    linkedData
  }

  def getLinkedPayeData(uwks: RDD[UbrnWithKey],
                        appConfig: AppConfig, sc: SparkContext): RDD[UbrnWithData] = {

    // Company/VAT/PAYE: format data as (key, data) pairs so we can use RDD joins below

    val pqReader = new PayeRecsParquetReader(sc)
    val payes: RDD[(String, PayeRec)] = pqReader.loadFromParquet(appConfig)

    // Join Links to corresponding data

    val linkedData: RDD[UbrnWithData] = uwks.filter { r => r.src == PAYE }.map { r => (r.key, r) }
      .join(payes)
      .map { case (key, (uwk, data))
      => UbrnWithData(uwk.ubrn, uwk.src, data)
      }

    linkedData
  }


  def getLinksAsUwks(appConfig: AppConfig, sc: SparkContext): RDD[UbrnWithKey] = {
    // Load Links from Parquet
    val linkRecsReader = new ProcessedLinksParquetReader(sc)
    val links: RDD[LinkRec] = linkRecsReader.loadFromParquet(appConfig)

    // explodeLink() converts each nested Link record to a sequence of (UBRN, type, key) triples.
    // flatMap(identity) then turns it from an RDD[Seq[UbrnWithKey]] into an
    // RDD[UbrnWithKey], which is what we want.

    val uwks: RDD[UbrnWithKey] = links.map { ln => Transformers.explodeLink(ln) }.flatMap(identity)
    uwks
  }

  // ***************** MAIN BI linking process below **************************

  def buildLinkedBusinessIndexRecords(sc: SparkContext, appConfig: AppConfig) = {

    // Load Links from Parquet and convert to Ubrn With Key structure
    val uwks: RDD[UbrnWithKey] = getLinksAsUwks(appConfig, sc)

    // Cache this data as we will be doing different things to it below
    uwks.cache()

    // Join Links to corresponding company/VAT/PAYE data

    val companyData: RDD[UbrnWithData] = getLinkedCompanyData(uwks, appConfig, sc)

    val vatData: RDD[UbrnWithData] = getLinkedVatData(uwks, appConfig, sc)

    val payeData: RDD[UbrnWithData] = getLinkedPayeData(uwks, appConfig, sc)

    // Put the lists of UWDs  (UBRN, src, data) back together
    val combined: RDD[UbrnWithData] = companyData ++ vatData ++ payeData

    // Now we can group data for same UBRN back together to make Business records
    val businessRecords: RDD[Business] = convertUwdsToBusinessRecords(combined)

    // Now we can convert Business records to Business Index entries

    val businessIndexes: RDD[BusinessIndex] = businessRecords.map(Transformers.convertToBusinessIndex)

    // write BI data to parquet file
    writeBiRddToParquet(sc, appConfig, businessIndexes)

    // clear cached UWKs
    uwks.unpersist()
  }

}
