package uk.gov.ons.bi.dataload.model

/**
  * Created by websc on 16/02/2017.
  */
// Data elements:  most fields are optional because they may not be present in source files.

case class LinkRec(ubrn: String, ch: Option[String], vat: Option[Seq[String]], paye: Option[Seq[String]])

// BusinessElement trait allows us to mix different data records in Ubrn... classes below
sealed trait BusinessElement

case class CompanyRec(companyNo: Option[String], companyName: Option[String],
                      companyStatus: Option[String], sicCode1: Option[String], postcode: Option[String]
                     ) extends BusinessElement

case class VatRec(vatRef: Option[Long], nameLine1: Option[String], postcode: Option[String],
                  sic92: Option[Int], legalStatus: Option[Int], turnover: Option[Long]) extends BusinessElement

case class PayeRec(payeRef: Option[String], nameLine1: Option[String], postcode: Option[String],
                   legalStatus: Option[Int], decJobs: Option[Double], marJobs: Option[Double],
                   junJobs: Option[Double], sepJobs: Option[Double], jobsLastUpd: Option[String])
  extends BusinessElement

// These are intermediate structures that we use during the main Spark "link-and-join" processing.

case class Business(ubrn: String, company: Option[CompanyRec],
                    vat: Option[Seq[VatRec]], paye: Option[Seq[PayeRec]])

case class UbrnWithKey(ubrn: String, src: BIDataSource, key: String)

case class UbrnWithData(ubrn: String, src: BIDataSource, data: BusinessElement)

case class UbrnWithList(ubrn: String, data: Seq[UbrnWithData])

case class BusinessIndex(ubrn: String, companyName: Option[String], postcode: Option[String],
                         industryCode: Option[String], legalStatus: Option[String],
                         totalTurnover: Option[Long], totalNumEmps: Option[Double])

