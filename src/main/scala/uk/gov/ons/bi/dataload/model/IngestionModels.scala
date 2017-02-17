package uk.gov.ons.bi.dataload.model

/**
  * Created by websc on 16/02/2017.
  */
// Source types


// Data elements:

case class LinkRec(ubrn: String, ch: Option[String], vat: Option[Seq[String]], paye: Option[Seq[String]])

sealed trait BusinessElement

case class CompanyRec(companyNo: String, companyName: String,
                      companyStatus: String, sicCode1: String, postcode: String
                     ) extends BusinessElement

case class VatRec(vatRef: Long, nameLine1: String, postcode: String,
                  sic92: Int, legalStatus: Int, turnover: Long) extends BusinessElement

case class PayeRec(payeRef: String, nameLine1: String, postCode: String, legalStatus: Int,
                   decJobs: Double, marJobs: Double, junJobs: Double, sepJobs: Double, jobsLastUpd: String)
  extends BusinessElement

// Construct these:

case class Business(ubrn: String, company: Option[CompanyRec],
                    vat: Option[Seq[VatRec]], paye: Option[Seq[PayeRec]])

case class UbrnWithKey(ubrn: String, src: BIDataSource, key: String)

case class UbrnWithData(ubrn: String, src: BIDataSource, data: BusinessElement)

case class UbrnWithList(ubrn: String, data: Seq[UbrnWithData])

case class BusinessIndex(ubrn: String, companyName: Option[String], postcode: Option[String],
                         industryCode: Option[String], legalStatus: Option[String],
                         totalTurnover: Option[Long])

