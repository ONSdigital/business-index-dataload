package uk.gov.ons.bi.dataload.utils


import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import uk.gov.ons.bi.dataload.model._

import scala.util.{Success, Try}

/**
  * Created by websc on 24/02/2017.
  */
object Transformers {

  def extractNumericSicCode(sic: String): Long = {
    // Extracts numeric SIC code, assuming it is first element in string
    val NumStartRegex = "(\\d+).*".r

    sic match {
      case NumStartRegex(x) => x.toLong
      case _ => 0L
    }
  }

  // Convert the grouped UBRN + Lists into Business records

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

  // Each BI field needs to be extracted from Business record using corresponding rules

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
    candidates.foldLeft[Option[String]](None)(_ orElse _).map(_.toUpperCase)
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

  def getPostcodeArea(pc: Option[String]): Option[String] = {
    // Postcode area is first character portion of postcode.
    // e.g. G12 1AB --> G
    //      CF12 8AB --> CF
    val pattern = "[A-Z]+".r
    pc.flatMap(pattern.findFirstIn(_))
  }

  def getIndustryCode(br: Business): Option[Long] = {

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
    val indCode = candidates.foldLeft[Option[String]](None)(_ orElse _)

    indCode.map(extractNumericSicCode)
  }

  def getTradingStatus(br: Business): Option[String] = {
    // Extract potential values from CH
    br.company.flatMap {
      _.companyStatus
    }
  }

  def getLegalStatus(br: Business): Option[String] = {
    // Extract potential values from VAT/PAYE records
    // Take first VAT/PAYE record (if any)
    val vat: Option[String] = br.vat.flatMap { vs => vs.headOption }.flatMap {
      _.legalStatus.map(_.toString)
    }
    val paye: Option[String] = br.paye.flatMap { ps => ps.headOption }.flatMap {
      _.legalStatus.map(_.toString)
    }

    // list in order of preference
    val candidates = Seq(vat, paye)
    // Take first non-empty name value from list
    candidates.foldLeft[Option[String]](None)(_ orElse _)
  }

  def getVatTurnover(br: Business): Option[Long] = {
    // not clear what rule is for deriving this. just take 1st one for now.
    br.vat.flatMap { vs => vs.headOption }.flatMap {
      _.turnover
    }
  }

  def getVatTotalTurnover(br: Business): Option[Long] = {
    // not clear what rule is for deriving this. Add up all VAT turnovers?
    br.vat match {
      case Some(vats: Seq[VatRec]) => Option(vats.map(_.turnover.getOrElse(0L)).sum)
      case _ => None
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
    // e.g. if jobsLastUp is "Jun15" we want to get the June num of employees.
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

  def getNumEmployees(br: Business): Option[Int] = {
    // not clear what rule is for deriving this.
    val payes: Seq[PayeRec] = br.paye.getOrElse(Nil)
    val jobUpdates: Seq[(Option[DateTime], Option[Double])] = payes.map { p => getLatestJobsForPayeRec(p) }
    // Allow for empty sequence, or multiple PAYE entries, get num emps for most recent date.
    // - need to provide explicit sort order for tuple.
    val ju: Option[Double] = jobUpdates.sortBy {
      case (Some(dt: DateTime), _) => dt.getMillis
      case _ => -1
    }
      .reverse match {
      case Nil => None
      case xs => xs.head._2
    }
    // BI mapping expects an integer
    ju.map(_.toInt)
  }

  def getVatRefs(br: Business): Option[Seq[Long]] = {
    // VAT Refs may not be present
    Option(br.vat.getOrElse(Nil).flatMap { v => v.vatRef })
  }

  def getPayeRefs(br: Business): Option[Seq[String]] = {
    // PAYE Refs may not be present
    Option(br.paye.getOrElse(Nil).flatMap { p => p.payeRef })
  }

  def convertToBusinessIndex(br: Business): BusinessIndex = {

    val businessName: Option[String] = getCompanyName(br)
    val postcode: Option[String] = getPostcode(br)
    val postcodeArea: Option[String] = getPostcodeArea(postcode)
    val industryCode: Option[Long] = getIndustryCode(br)
    val legalStatus: Option[String] = getLegalStatus(br)
    val tradingStatus: Option[String] = getTradingStatus(br)

    val tradingStatusBand = BandMappings.tradingStatusBand(tradingStatus)

    // Not clear what rule is for deriving this:
    val turnover: Option[Long] = getVatTotalTurnover(br)

    // Derive Turnover Band for BI
    val turnoverBand: Option[String] = BandMappings.turnoverBand(turnover)

    // Not clear how we calculate employees
    val numEmps: Option[Int] = getNumEmployees(br)

    // Derive Employment Band for BI
    val empBand: Option[String] = BandMappings.employmentBand(numEmps)

    // include CompanyNo

    val companyNo: Option[String] = br.company.flatMap(_.companyNo)

    // Include *all* PAYE and VAT Refs
    val vatRefs: Option[Seq[Long]] = getVatRefs(br)
    val payeRefs: Option[Seq[String]] = getPayeRefs(br)

    // Build a BI record that we can later upload to ElasticSource
    // Use postcode area instead of full postcode in index
    BusinessIndex(br.ubrn, businessName, postcodeArea, industryCode, legalStatus,
      tradingStatusBand, turnoverBand, empBand, companyNo, vatRefs, payeRefs)
  }

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
}
