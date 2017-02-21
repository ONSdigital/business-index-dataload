package uk.gov.ons.bi.dataload.linker

import com.github.nscala_time.time.Imports._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.joda.time.DateTime
import uk.gov.ons.bi.dataload.model._
import uk.gov.ons.bi.dataload.reader.ParquetReader
import uk.gov.ons.bi.dataload.utils.AppConfig

import scala.util.{Success, Try}


/**
  * Created by websc on 16/02/2017.
  */
object LinkedBusinessBuilder {
  // This needs to be an object, not a Singleton, because we get weird Spark "Task not serializable"
  // errors when there is a lot of nested RDD processing around here. Might be better in Spark 2.x?


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

  // Now convert the grouped UWLs into Business records

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
    // Extract potential values from CH/VAT/PAYE records
    // Take first VAT/PAYE record (if any)
    val co: Option[String] = br.company.flatMap {
      _.companyName
    }
    val vat: Option[String] = br.vat.flatMap { vs => vs.headOption }.flatMap {
      _.nameLine1
    }
    val paye: Option[String] = br.paye.flatMap { ps => ps.headOption }.flatMap {
      _.nameLine1
    }
    // list in order of preference
    val candidates = Seq(co, vat, paye)
    // Take first non-empty name value from list
    candidates.foldLeft[Option[String]](None)(_ orElse _)
  }

  def getPostcode(br: Business): Option[String] = {
    // Extract potential values from CH/VAT/PAYE records
    // Take first VAT/PAYE record (if any)
    val co: Option[String] = br.company.flatMap {
      _.postcode
    }
    val vat: Option[String] = br.vat.flatMap { vs => vs.headOption }.flatMap {
      _.postcode
    }
    val paye: Option[String] = br.paye.flatMap { ps => ps.headOption }.flatMap {
      _.postcode
    }

    // list in order of preference
    val candidates = Seq(co, vat, paye)
    // Take first non-empty name value from list
    candidates.foldLeft[Option[String]](None)(_ orElse _)
  }

  def getIndustryCode(br: Business): Option[String] = {

    // Extract potential values from CH/VAT records
    // Take first VATrecord (if any)
    val co: Option[String] = br.company.flatMap {
      _.sicCode1
    }
    val vat: Option[String] = br.vat.flatMap { vs => vs.headOption }.flatMap {
      _.sic92.map(_.toString)
    }

    // list in order of preference
    val candidates = Seq(co, vat)
    // Take first non-empty name value from list
    candidates.foldLeft[Option[String]](None)(_ orElse _)
  }

  def getLegalStatus(br: Business): Option[String] = {
    // Extract potential values from CH/VAT/PAYE records
    // Take first VAT/PAYE record (if any)
    val co: Option[String] = br.company.flatMap {
      _.companyStatus
    }
    val vat: Option[String] = br.vat.flatMap { vs => vs.headOption }.flatMap {
      _.legalStatus.map(_.toString)
    }
    val paye: Option[String] = br.paye.flatMap { ps => ps.headOption }.flatMap {
      _.legalStatus.map(_.toString)
    }

    // list in order of preference
    val candidates = Seq(co, vat, paye)
    // Take first non-empty name value from list
    candidates.foldLeft[Option[String]](None)(_ orElse _)
  }

  def getVatTurnover(br: Business): Option[Long] = {
    // not clear what rule is for deriving this. just take 1st one for now.
    br.vat.flatMap { vs => vs.headOption }.flatMap {
      _.turnover
    }
  }

  def getLastUpdOpt(str: Option[String]): Option[DateTime] = {
    // Now convert the string to a MMMyy date (if possible)
    val fmt = DateTimeFormat.forPattern("MMMyy")
    // unpack the Option
    str.getOrElse("") match {
      case "" => None
      case s: String => Try {
        DateTime.parse(s, fmt)
      } match {
        case Success(d: DateTime) => Some(d)
        case _ => None
      }
    }
  }

  def getLatestJobsForPayeRec(rec: PayeRec): (Option[DateTime], Option[Double]) = {

    // Convert jobsLastUpd string to date if possible
    val upd: Option[DateTime] = getLastUpdOpt(rec.jobsLastUpd)

    // Use this to get corresponding jobs value from record
    val jobs: Option[Double] = upd.flatMap { d =>
      d.getMonthOfYear match {
        case 3 => rec.marJobs
        case 6 => rec.junJobs
        case 9 => rec.sepJobs
        case 12 => rec.decJobs
        case _ => None
      }
    }
    (upd, jobs)
  }

  def getNumEmployees(br: Business): Option[Double] = {
    // not clear what rule is for deriving this.
    val payes: Seq[PayeRec] = br.paye.getOrElse(Nil)
    val jobUpdates: Seq[(Option[DateTime], Option[Double])] = payes.map { p => getLatestJobsForPayeRec(p) }
    // Allow for empty sequence, get num emps for most recent date
    jobUpdates.sorted.reverse match {
      case Nil => None
      case xs => xs.head._2
    }
  }


  def convertToBusinessIndex(br: Business): BusinessIndex = {

    val companyName: Option[String] = getCompanyName(br)
    val postcode: Option[String] = getPostcode(br)
    val industryCode: Option[String] = getIndustryCode(br)

    // status needs to be string because CompanyStatus is a string in source data
    val legalStatus: Option[String] = getLegalStatus(br)

    // not clear what rule is for deriving this:
    val totalTurnover: Option[Long] = getVatTurnover(br)

    // Not clear how we calculate employees
    val numEmps: Option[Double] = getNumEmployees(br)

    // Build a BI record that we can later upload to ElasticSource
    BusinessIndex(br.ubrn, companyName, postcode, industryCode,
      legalStatus, totalTurnover, numEmps)
  }


  def buildLinkedBusinessIndexRecords(implicit sc: SparkContext, appConfig: AppConfig) = {

    val sqlContext = new SQLContext(sc)

    // Load Parquet source data and links
    val pqReader = new ParquetReader

    // Company/VAT/PAYE: format data as (key, data) pairs so we can use RDD joins below

    val vats: RDD[(String, VatRec)] = pqReader.loadVatRecsFromParquet(appConfig)

    val payes: RDD[(String, PayeRec)] = pqReader.loadPayeRecsFromParquet(appConfig)

    val cos: RDD[(String, CompanyRec)] = pqReader.loadCompanyRecsFromParquet(appConfig)

    val links: RDD[LinkRec] = pqReader.loadLinkRecsFromParquet(appConfig)

    // explodeLink() converts each nested Link record to a sequence of (UBRN, type, key) triples.
    // flatMap(identity) then turns it from an RDD of Seqs of UbrnWithKey into an
    // RDD of UbrnWithKey, which is what we want.

    val uwks = links.map { ln => explodeLink(ln) }.flatMap(identity)

    // Cache this data as we will be doing different things to it
    uwks.cache()

    // Join Links to corresponding company/VAT/PAYE data

    val companyData: RDD[UbrnWithData] = uwks.filter { r => r.src == CH }.map { r => (r.key, r) }
      .join(cos)
      .map { case (key, (uwk, data))
      => UbrnWithData(uwk.ubrn, uwk.src, data)
      }

    val vatData: RDD[UbrnWithData] = uwks.filter { r => r.src == VAT }.map { r => (r.key, r) }
      .join(vats)
      .map { case (key, (uwk, data))
      => UbrnWithData(uwk.ubrn, uwk.src, data)
      }

    val payeData: RDD[UbrnWithData] = uwks.filter { r => r.src == PAYE }.map { r => (r.key, r) }
      .join(payes)
      .map { case (key, (uwk, data))
      => UbrnWithData(uwk.ubrn, uwk.src, data)
      }

    // Put the lists of UWDs  (UBRN, src, data) back together
    val combined: RDD[UbrnWithData] = companyData ++ vatData ++ payeData

    // Now we can group data for same UBRN back together
    val grouped: RDD[(String, Iterable[UbrnWithData])] = combined.map { r => (r.ubrn, r) }.groupByKey()
    val uwls: RDD[UbrnWithList] = grouped.map { case (ubrn, uwds) => UbrnWithList(ubrn, uwds.toList) }


    val businessRecords = uwls.map(buildBusinessRecord)

    // Now we can convert Business records to Business Index entries

    val businessIndexes = businessRecords.map(convertToBusinessIndex)

    businessIndexes.cache()

    uwks.unpersist()

    // Need some voodoo here to convert RDD[BusinessIndex] back to DataFrame

    val biSchema = StructType(Seq(
      StructField("ubrn", StringType, true),
      StructField("companyName", StringType, true),
      StructField("postcode", StringType, true),
      StructField("industryCode", StringType, true),
      StructField("legalStatus", StringType, true),
      StructField("totalTurnover", LongType, true),
      StructField("totalNumEmployees", DoubleType, true)
    ))

    def biRowMapper(bi: BusinessIndex): Row = {
      Row(bi.ubrn, bi.companyName, bi.postcode, bi.industryCode, bi.legalStatus, bi.totalTurnover, bi.totalNumEmps)
    }

    val biRows: RDD[Row] = businessIndexes.map(biRowMapper)

    val biDf: DataFrame = sqlContext.createDataFrame(biRows, biSchema)

    // Write BI DataFrame to Parquet file. We will load it into ElasticSearch separately.

    val parquetDataConfig = appConfig.ParquetDataConfig
    val parquetPath = parquetDataConfig.dir
    val parquetBiFile = parquetDataConfig.bi
    val biFile = s"$parquetPath/$parquetBiFile"

    biDf.write.mode("overwrite").parquet(biFile)

    businessIndexes.unpersist()

  }

}
