package uk.gov.ons.bi.dataload.linker

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import uk.gov.ons.bi.dataload.utils.AppConfig
import uk.gov.ons.bi.dataload.model._
import uk.gov.ons.bi.dataload.reader.ParquetReader

/**
  * Created by websc on 16/02/2017.
  */
object LinkedBusinessBuilder {
  // Make this an object to avoid weird serialization errors.

  // Trying to use implicit voodoo to make SC available
  implicit val sc = SparkContext.getOrCreate(new SparkConf().setAppName("ONS BI Dataload: Link to Business Data"))

  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  def explodeLink(ln: LinkRec): Seq[UbrnWithKey] = {
    // Convert the Link into a list of UBRNs with business data keys
    val company = ln.ch match {
      case Some(companyNo) => Seq(UbrnWithKey(ln.ubrn, CH, companyNo))
      case _ => Nil
    }

    val vat: Seq[UbrnWithKey] = ln.vat.map { vatrefs =>
      vatrefs.map { vatref => UbrnWithKey(ln.ubrn, VAT, vatref) }
    }.getOrElse(Nil)
    val paye: Seq[UbrnWithKey] = ln.paye.map { payerefs =>
      payerefs.map { payeref => UbrnWithKey(ln.ubrn, PAYE, payeref) }
    }.getOrElse(Nil)

    company ++ vat ++ paye
  }

  def buildLinkedBusinessIndexRecords(appConfig: AppConfig) = {

    // Load source data and links
    val pqReader = new ParquetReader

    val links: RDD[LinkRec] = pqReader.loadLinkRecsFromParquet(appConfig)

    val vats: RDD[(String, VatRec)] = pqReader.loadVatRecsFromParquet(appConfig)

    val payes: RDD[(String, PayeRec)] = pqReader.loadPayeRecsFromParquet(appConfig)

    val cos: RDD[(String, CompanyRec)] = pqReader.loadCompanyRecsFromParquet(appConfig)

    // Convert each Link record to a sequence of simpler (UBRN, type, key) triples.
    // The flatMap(identity) turns it from an RDD of Lists of UbrnWithKey into an
    // RDD of UbrnWithKey, which is what we want.

    val uwks = links.map { ln => explodeLink(ln) }.flatMap(identity)

    // Cache this data as we will be doing different things to it
    uwks.cache()

    println(s"UWKs contains ${uwks.count} records.")

    val companyData: RDD[UbrnWithData] = uwks.filter { r => r.src == CH }.map { r => (r.key, r) }
      .join(cos)
      .map { case (key, (uwk, data))
      => UbrnWithData(uwk.ubrn, uwk.src, data)
      }

    println(s"companyData contains ${companyData.count} records.")

    val vatData: RDD[UbrnWithData] = uwks.filter { r => r.src == VAT }.map { r => (r.key, r) }
      .join(vats)
      .map { case (key, (uwk, data))
      => UbrnWithData(uwk.ubrn, uwk.src, data)
      }

    println(s"vatData contains ${vatData.count} records.")

    val payeData: RDD[UbrnWithData] = uwks.filter { r => r.src == PAYE }.map { r => (r.key, r) }
      .join(payes)
      .map { case (key, (uwk, data))
      => UbrnWithData(uwk.ubrn, uwk.src, data)
      }

    println(s"payeData contains ${payeData.count} records.")

    val combined = companyData ++ vatData ++ payeData

    val grouped: RDD[(String, Iterable[UbrnWithData])] = combined.map { r => (r.ubrn, r) }.groupByKey()

    val uwls = grouped.map { case (ubrn, uwds) => UbrnWithList(ubrn, uwds.toList) }

    println(s"UWLs contains ${uwls.count} grouped records.")

    // Now convert to Business records

    def buildBusinessRecord(uwl: UbrnWithList) = {
      val ubrn = uwl.ubrn
      // Should only be ONE company
      val company: Option[CompanyRec] = uwl.data.filter { r => r.src == CH }
        .map { case UbrnWithData(u, CH, data: CompanyRec) => data }.headOption
      val vats: Option[Seq[VatRec]] = Some(uwl.data.filter { r => r.src == VAT }
        .map { case UbrnWithData(u, VAT, data: VatRec) => data })
      val payes: Option[Seq[PayeRec]] = Some(uwl.data.filter { r => r.src == PAYE }
        .map { case UbrnWithData(u, PAYE, data: PayeRec) => data })

      Business(ubrn, company, vats, payes)

    }

    def getCompanyName(br: Business): Option[String] = {
      (br.company, br.vat, br.paye) match {
        case (Some(co: CompanyRec), _, _) => Some(co.companyName)
        case (_, Some(vats: Seq[VatRec]), _) => Some(vats.head.nameLine1)
        case (_, _, Some(payes: Seq[PayeRec])) => Some(payes.head.nameLine1)
        case _ => None
      }
    }

    def getPostcode(br: Business): Option[String] = {
      (br.company, br.vat, br.paye) match {
        case (Some(co: CompanyRec), _, _) => Some(co.postcode)
        case (_, Some(vats: Seq[VatRec]), _) => Some(vats.head.postcode)
        case (_, _, Some(payes: Seq[PayeRec])) => Some(payes.head.postCode)
        case _ => None
      }
    }

    def getIndustryCode(br: Business): Option[String] = {
      (br.company, br.vat) match {
        case (Some(co: CompanyRec), _) => Some(co.sicCode1)
        case (_, Some(vats: Seq[VatRec])) => Some(vats.head.sic92.toString)
        case _ => None
      }
    }

    def getLegalStatus(br: Business): Option[String] = {
      (br.company, br.vat, br.paye) match {
        case (Some(co: CompanyRec), _, _) => Some(co.companyStatus)
        case (_, Some(vats: Seq[VatRec]), _) => Some(vats.head.legalStatus.toString)
        case (_, _, Some(payes: Seq[PayeRec])) => Some(payes.head.legalStatus.toString)
        case _ => None
      }
    }

    def convertToBusinessIndex(br: Business): BusinessIndex = {

      val companyName: Option[String] = getCompanyName(br)
      val postcode: Option[String] = getPostcode(br)
      val industryCode: Option[String] = getIndustryCode(br)

      // status needs to be string because CompanyStatus is a string in source data
      val legalStatus: Option[String] = getLegalStatus(br)

      // not clear what rule is for deriving this:
      val totalTurnover: Option[Long] = br.vat.map { vats => vats.map(_.turnover).sum }

      // Build a BI record that we can later upload to ElasticSource
      BusinessIndex(br.ubrn, companyName, postcode, industryCode,
        legalStatus, totalTurnover)
    }

    val businessRecords = uwls.map(buildBusinessRecord)


    val businessIndexes = businessRecords.map(convertToBusinessIndex)

    /* Need some voodoo here to convert RDD[BusinessIndex] back to DataFrame */

    val biSchema = StructType(Seq(
      StructField("ubrn", StringType, true),
      StructField("companyName", StringType, true),
      StructField("postcode", StringType, true),
      StructField("industryCode", StringType, true),
      StructField("legalStatus", StringType, true),
      StructField("totalTurnover", LongType, true)
    ))

    def biRowMapper(bi: BusinessIndex): Row = {
      Row(bi.ubrn, bi.companyName, bi.postcode, bi.industryCode, bi.legalStatus, bi.totalTurnover)
    }

    val biRows: RDD[Row] = businessIndexes.map(biRowMapper)

    val biDf: DataFrame = sqlContext.createDataFrame(biRows, biSchema)


    // **** TEMPORARY:  Write BI data to Parquet file

    val parquetDataConfig = appConfig.ParquetDataConfig
    val parquetPath = parquetDataConfig.dir
    val biFile = s"$parquetPath/BUSINESS_INDEX_OUTPUT.parquet"

    biDf.printSchema()

    biDf.write.mode("overwrite").parquet(biFile)

    uwks.unpersist()
  }

}
