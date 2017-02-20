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

  def buildLinkedBusinessIndexRecords(implicit sc: SparkContext, appConfig: AppConfig) = {

    val sqlContext = new SQLContext(sc)

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

    val combined: RDD[UbrnWithData] = companyData ++ vatData ++ payeData

    val grouped: RDD[(String, Iterable[UbrnWithData])] = combined.map { r => (r.ubrn, r) }.groupByKey()

    val uwls: RDD[UbrnWithList] = grouped.map { case (ubrn, uwds) => UbrnWithList(ubrn, uwds.toList) }

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

    val businessRecords = uwls.map(buildBusinessRecord)


    // Next we convert Business records to Business Index entries
    //
    def getCompanyName(br: Business): Option[String] = {
      (br.company, br.vat, br.paye) match {
        case (Some(co: CompanyRec), _, _) => co.companyName
        case (_, Some(vats: Seq[VatRec]), _) =>
          vats.headOption match {
            case Some(r: VatRec) => r.nameLine1
            case _ => None
          }
        case (_, _, Some(payes: Seq[PayeRec])) => payes.headOption match {
          case Some(r: PayeRec) => r.nameLine1
          case _ => None
        }
        case _ => None
      }
    }

    def getPostcode(br: Business): Option[String] = {
      (br.company, br.vat, br.paye) match {
        case (Some(co: CompanyRec), _, _) => co.postcode
        case (_, Some(vats: Seq[VatRec]), _) => vats.headOption match {
          case Some(r: VatRec) => r.postcode
          case _ => None
        }
        case (_, _, Some(payes: Seq[PayeRec])) => payes.headOption match {
          case Some(r: PayeRec) => r.postCode
          case _ => None
        }
        case _ => None
      }
    }

    def getIndustryCode(br: Business): Option[String] = {
      (br.company, br.vat) match {
        case (Some(co: CompanyRec), _) => co.sicCode1
        case (_, Some(vats: Seq[VatRec])) => vats.headOption match{
          case Some(r: VatRec) => r.sic92.map(_.toString)
          case _ => None
        }
        case _ => None
      }
    }

    def getLegalStatus(br: Business): Option[String] = {
      (br.company, br.vat, br.paye) match {
        case (Some(co: CompanyRec), _, _) => co.companyStatus
        case (_, Some(vats: Seq[VatRec]), _) => vats.headOption match{
          case Some(r: VatRec) => r.legalStatus.map(_.toString)
          case _ => None
        }
        case (_, _, Some(payes: Seq[PayeRec])) => payes.headOption match{
          case Some(r: PayeRec) => r.legalStatus.map(_.toString)
          case _ => None
        }
        case _ => None
      }
    }

    def getVatTurnover(br: Business): Option[Long] = {
      // not clear what rule is for deriving this
      br.vat.map { (vats: Seq[VatRec]) =>
        vats.map(v => v.turnover.getOrElse(0L)).sum
      }
    }


    def getLastUpdOpt(str: Option[String]): Option[DateTime] = {

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

    def getLatestJobsForPayeRec(rec: PayeRec) = {

      // Convert josbLastUpd string to date
      val upd: Option[DateTime] = getLastUpdOpt(rec.jobsLastUpd)

      // Use this to get corresponding jobd value from record
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
      // not clear what rule is for deriving this
      val payes: Seq[PayeRec] = br.paye.getOrElse(Nil)
      val jobUpdates: Seq[(Option[DateTime], Option[Double])] = payes.map { p => getLatestJobsForPayeRec(p) }
      // Allow for empty sequence
      jobUpdates match {
        case Nil => None
        case xs => xs.reverse.head._2
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


    val businessIndexes = businessRecords.map(convertToBusinessIndex)

    businessIndexes.cache()

    uwks.unpersist()

    sc.parallelize(Seq(s"businessIndexes contains ${businessIndexes.count} grouped records.")).saveAsTextFile("./DUMMY")

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
      Row(bi.ubrn, bi.companyName, bi.postcode, bi.industryCode, bi.legalStatus, bi.totalTurnover)
    }

    val biRows: RDD[Row] = businessIndexes.map(biRowMapper)

    val biDf: DataFrame = sqlContext.createDataFrame(biRows, biSchema)


    // **** TEMPORARY:  Write BI data to Parquet file

    val parquetDataConfig = appConfig.ParquetDataConfig
    val parquetPath = parquetDataConfig.dir
    val parquetBiFile = parquetDataConfig.bi
    val biFile = s"$parquetPath/$parquetBiFile"

    biDf.printSchema()

    println(s"Writing Business Indexes to $biFile")

    biDf.write.mode("overwrite").parquet(biFile)

    businessIndexes.unpersist()

  }

}
