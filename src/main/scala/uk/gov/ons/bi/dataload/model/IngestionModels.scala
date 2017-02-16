package uk.gov.ons.bi.dataload.model

/**
  * Created by websc on 16/02/2017.
  */
// Source types



// Data elements:

case class LinkRec(ubrn: String, ch: Option[String], vat:Option[Seq[String]], paye: Option[Seq[String]])

sealed trait BusinessElement

case class CompanyRec(companyNo: String, companyName: String,
companyStatus: String, sicCode1: String, postcode: String
                     ) extends BusinessElement

case class VatRec(vatRef: String, turnover: Int) extends BusinessElement


/*
  df.select(
      $"entref".as("paye_entref"),
      $"payeref",
      $"nameline1".as("paye_name"),
      $"inqcode".as("paye_inqcode"),
      $"legalstatus".as("paye_legalstatus"),
      $"employer_cat".as("paye_employer_cat"),
      $"postcode".as("paye_postcode")
    )
 */
case class PayeRec(payeRef: String, entRef: String) extends BusinessElement

// Construct these:

case class Business(ubrn: String, company: Option[CompanyRec], vat:Option[Seq[VatRec]], paye: Option[Seq[PayeRec]])

case class UbrnWithKey (ubrn: String,src: BIDataSource, key: String)

case class UbrnWithData (ubrn: String,src: BIDataSource, data: BusinessElement)

case class UbrnWithList(ubrn: String, data:Seq[UbrnWithData])

case class BusinessIndex(ubrn: String, companyNo: Option[String], companyName: Option[String],
                         totalTurnover: Option[Int], avgEmps: Option[Double])
