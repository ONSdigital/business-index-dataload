package uk.gov.ons.bi.dataload.model

/**
  * Created by websc on 16/02/2017.
  */

// Data elements:  most fields are optional because they may not be present in source files.

// BusinessElement trait allows us to mix different data records in Ubrn... classes below
sealed trait BusinessElement

case class CompanyRec(companyNo: Option[String], companyName: Option[String],
                      companyStatus: Option[String], sicCode1: Option[String], postcode: Option[String]
                     ) extends BusinessElement

case class VatRec(vatRef: Option[Long], nameLine1: Option[String], postcode: Option[String],
                  sic92: Option[Int], legalStatus: Option[Int],
                  turnover: Option[Long], deathcode: Option[String] = None) extends BusinessElement

case class PayeRec(payeRef: Option[String], nameLine1: Option[String], postcode: Option[String],
                   legalStatus: Option[Int], decJobs: Option[Double], marJobs: Option[Double],
                   junJobs: Option[Double], sepJobs: Option[Double], jobsLastUpd: Option[String],
                   stc: Option[Int] = None, sic: Option[Int] = None, deathcode: Option[String] = None)
  extends BusinessElement


// These are intermediate structures that we use during the main Spark "link-and-join" processing.

case class TcnSicLookup(tcn: Int, sic: Int)

case class Business(ubrn: BiTypes.Ubrn, company: Option[CompanyRec],
                    vat: Option[Seq[VatRec]], paye: Option[Seq[PayeRec]])

case class UbrnWithKey(ubrn: BiTypes.Ubrn, src: BIDataSource, key: String)

case class UbrnWithData(ubrn: BiTypes.Ubrn, src: BIDataSource, data: BusinessElement)

case class UbrnWithList(ubrn: BiTypes.Ubrn, data: Seq[UbrnWithData])

case class BusinessIndex(ubrn: BiTypes.Ubrn, businessName: Option[String], postCode: Option[String],
                         industryCode: Option[Long], legalStatus: Option[String], tradingStatus: Option[String],
                         turnoverBand: Option[String], employmentBand: Option[String], companyNo: Option[String],
                         vatRefs: Option[Seq[Long]], payeRefs: Option[Seq[String]])

