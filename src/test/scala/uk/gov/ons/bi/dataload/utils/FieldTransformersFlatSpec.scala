package uk.gov.ons.bi.dataload.utils

import org.joda.time.DateTime
import org.scalatest._
import uk.gov.ons.bi.dataload.model.{Business, CompanyRec, PayeRec, VatRec}

/**
  * Created by websc on 27/02/2017.
  */

class FieldTransformersFlatSpec extends FlatSpec with Matchers {

  val fullCompanyRec = CompanyRec(Some("Company1"), Some("CompanyOne"),
                       Some("Active"), Some("12345 - fubar"), Some("Company Post Code"),
  Some("address1"), Some("address2"), Some("address3"), Some("address4"), Some("address5"))

  val fullVatRec = VatRec(Some(100L), Some("VAT Name Line 1"), Some("VAT Post Code"),
    Some("92"), Some(1), Some(12345), Some("3"),
    Some("address1"), Some("address2"), Some("address3"), Some("address4"), Some("address5"))
  )

  val fullPayeRec = PayeRec(Some("PAYE REF"), Some("PAYE Name Line 1"), Some("PAYE Post Code"),
    Some(2), Some(120.0D), Some(30.0D),
    Some(60.0D), Some(90.0D), Some("Jun16"), Some(100), Some("1500"),Some("6"),
    Some("address1"), Some("address2"), Some("address3"), Some("address4"), Some("address5"))


  "A Transformer" should "get latest job figure from PAYE record" in {

    val dt = Some(new DateTime("2016-06-01"))
    val expected = (dt, Some(60.0D))
    val result: (Option[DateTime], Option[Double]) = Transformers.getLatestJobsForPayeRec(fullPayeRec)

    result should be(expected)
  }

  "A Transformer" should "return correct Industry Code (from Company record)" in {

    val expected = Some("12345")

    val br = Business(100, Some(fullCompanyRec), None, None)
    val result = Transformers.getIndustryCode(br)

    result should be(expected)
  }

  "A Transformer" should "return correct total VAT turnover if VAT turnover present" in {

    val vat100 = fullVatRec.copy(turnover = Some(100L))
    val vat300 = fullVatRec.copy(turnover = Some(300L))
    val vatRecs = Some(Seq(vat100, vat300))
    val expected = Some(400L)
    val br = Business(100, None, vatRecs, None)
    val result = Transformers.getVatTotalTurnover(br)

    result should be(expected)
  }

  "A Transformer" should "return no total VAT turnover if no VAT turnover present" in {

    val vat100 = fullVatRec.copy(turnover = None)
    val vat300 = fullVatRec.copy(turnover = None)
    val vatRecs = Some(Seq(vat100, vat300))
    val expected = None
    val br = Business(100, None, vatRecs, None)
    val result = Transformers.getVatTotalTurnover(br)

    result should be(expected)
  }

  "A Transformer" should "return correct total VAT turnover if some VAT recs have no VAT turnover" in {

    val vat100 = fullVatRec.copy(turnover = Some(100L))
    val vat300 = fullVatRec.copy(turnover = None)
    val vatRecs = Some(Seq(vat100, vat300))
    val expected = Some(100L)
    val br = Business(100, None, vatRecs, None)
    val result = Transformers.getVatTotalTurnover(br)

    result should be(expected)
  }

  "A Transformer" should "return correct Industry Code (from VAT record)" in {

    val expected = fullVatRec.sic92
    val vatRecs = Some(Seq(fullVatRec))
    val br = Business(100, None, vatRecs, None)
    val result: Option[String] = Transformers.getIndustryCode(br)

    result should be(expected)
  }

  "A Transformer" should "return correct Industry Code (from PAYE record)" in {

    val expected = fullPayeRec.sic
    val recs = Some(Seq(fullPayeRec))
    val br = Business(100, None, None, recs)
    val result: Option[String] = Transformers.getIndustryCode(br)

    result should be(expected)
  }

  "A Transformer" should "return VAT Industry Code from Business with bad Company SIC, good VAT SIC" in {

    val expected = fullVatRec.sic92
    val co = fullCompanyRec.copy(sicCode1 = Some("X123 FUBAR"))
    val br = Business(100, Some(co), Some(Seq(fullVatRec)), None)
    val result = Transformers.getIndustryCode(br)

    result should be(expected)
  }

  "A Transformer" should "return PAYE SIC from Business with bad Company SIC, no VAT SIC" in {

    val co = fullCompanyRec.copy(sicCode1 = Some("X123 FUBAR"))
    val vat = fullVatRec.copy(sic92 = None)
    val paye = fullPayeRec
    val br = Business(100, Some(co), Some(Seq(vat)), Some(Seq(paye)))

    val expected = paye.sic
    val result = Transformers.getIndustryCode(br)

    result should be(expected)
  }

  "A Transformer" should "return None from Business with bad Company SIC, no VAT or PAYE SIC" in {

    val co = fullCompanyRec.copy(sicCode1 = Some("X123 FUBAR"))
    val vat = fullVatRec.copy(sic92 = None)
    val paye = fullPayeRec.copy(sic = None)
    val br = Business(100, Some(co), Some(Seq(vat)), Some(Seq(paye)))

    val expected = None
    val result = Transformers.getIndustryCode(br)

    result should be(expected)
  }

  "A Transformer" should "extract numeric SIC code correctly from string" in {

    val expected = Some("123")
    val sic = Some("123 FUBAR")
    val result: Option[String] = Transformers.extractNumericSicCode(sic)

    result should be(expected)
  }

  "A Transformer" should "extract numeric SIC code as None from bad SIC string" in {

    val expected = None
    val sic = Some("X123 FUBAR")
    val result: Option[String] = Transformers.extractNumericSicCode(sic)

    result should be(expected)
  }


  "A Transformer" should "return all VAT references" in {

    val vat1 = fullVatRec
    val vat2 = fullVatRec.copy(vatRef = Some(98765L))
    val vatRecs = Seq(vat1, vat2)
    val expected: Seq[Long] = vatRecs.flatMap(_.vatRef)
    val br = Business(100, None, Some(vatRecs), None)
    val result: Option[Seq[Long]] = Transformers.getVatRefs(br)

    result should be(Option(expected))
  }

  "A Transformer" should "return all PAYE references" in {

    val rec1 = fullPayeRec
    val rec2 = fullPayeRec.copy(payeRef = Some("PAYE2"))
    val recs = Seq(rec1, rec2)
    val expected: Seq[String] = recs.flatMap(_.payeRef)
    val br = Business(100, None, None, Some(recs))
    val result: Option[Seq[String]] = Transformers.getPayeRefs(br)

    result should be(Option(expected))
  }

  "A Transformer" should "return correct Legal Status (from VAT record)" in {

    val expected = fullVatRec.legalStatus.map(_.toString)
    val vatRecs = Some(Seq(fullVatRec))
    val br = Business(100, None, vatRecs, None)
    val result = Transformers.getLegalStatus(br)

    result should be(expected)
  }

  "A Transformer" should "return correct Legal Status if Company present with PAYE record" in {
    // New rule (JIRA ONSRBIB-526) is: if Company then legal status = 1, ignore others.
    val expected = Some("1")
    val payeRecs = Some(Seq(fullPayeRec))
    val br = Business(100, Some(fullCompanyRec), None, payeRecs)
    val result = Transformers.getLegalStatus(br)

    result should be(expected)
  }

  "A Transformer" should "return correct Legal Status if Company present with VAT record" in {
    // New rule (JIRA ONSRBIB-526) is: if Company then legal status = 1, ignore others.
    val expected = Some("1")
    val vatRecs = Some(Seq(fullVatRec))
    val br = Business(100, Some(fullCompanyRec), vatRecs, None)
    val result = Transformers.getLegalStatus(br)

    result should be(expected)
  }

  "A Transformer" should "return correct Legal Status (from PAYE record)" in {

    val expected = fullPayeRec.legalStatus.map(_.toString)
    val payeRecs = Some(Seq(fullPayeRec))
    val br = Business(100, None, None, payeRecs)
    val result = Transformers.getLegalStatus(br)

    result should be(expected)
  }

  "A Transformer" should "return correct Legal Status (from Company record)" in {

    val expected = Some("1")
    val br = Business(100, Some(fullCompanyRec), None, None)
    val result = Transformers.getLegalStatus(br)

    result should be(expected)
  }



  //===============================================================================

  "A Transformer" should "return correct Trading Status Band (from Company record)" in {

    val expected = Some("A")
    val br = Business(100, Some(fullCompanyRec), None, None)
    val result = Transformers.getTradingStatusBand(br)

    result should be(expected)
  }

  "A Transformer" should "return correct Trading Status Band (from VAT record)" in {
    val expected = Some("I")
    val vatRecs = Some(Seq(fullVatRec))
    val br = Business(100, None, vatRecs, None)
    val result = Transformers.getTradingStatusBand(br)

    result should be(expected)
  }

  "A Transformer" should "return correct Trading Status Band (from PAYE record)" in {
    val expected = Some("A")
    val payeRecs = Some(Seq(fullPayeRec))
    val br = Business(100, None, None, payeRecs)
    val result = Transformers.getTradingStatusBand(br)

    result should be(expected)
  }

  "A Transformer" should "return correct Trading Status Band where VAT is bad, PAYE is OK" in {
    val expected = Some("A") // PAYE rec deathcode should result in A
    val vatRecs = Some(Seq(fullVatRec.copy(deathcode = Some("BAD"))))
    val payeRecs = Some(Seq(fullPayeRec))

    val br = Business(100, None, vatRecs, payeRecs)
    val result = Transformers.getTradingStatusBand(br)

    result should be(expected)
  }

  "A Transformer" should "return correct Trading Status Band of None where all codes are bad" in {
    val expected = None

    val co = fullCompanyRec.copy(companyStatus = Some("BAD"))
    val vatRecs = Some(Seq(fullVatRec.copy(deathcode = Some("BAD"))))
    val payeRecs = Some(Seq(fullPayeRec.copy(deathcode = Some("BAD"))))

    val br = Business(100, Some(co), vatRecs, payeRecs)
    val result = Transformers.getTradingStatusBand(br)

    result should be(expected)
  }
  //===============================================================================

  "A Transformer" should "return correct Company Name (from Company record)" in {

    val expected = fullCompanyRec.companyName.map(_.toUpperCase)

    val br = Business(100, Some(fullCompanyRec), None, None)
    val result = Transformers.getCompanyName(br)

    result should be(expected)
  }


  "A Transformer" should "return correct Company Name (from VAT record)" in {

    val expected = fullVatRec.nameLine1.map(_.toUpperCase)
    val vatRecs = Some(Seq(fullVatRec))
    val br = Business(100, None, vatRecs, None)
    val result = Transformers.getCompanyName(br)

    result should be(expected)
  }

  "A Transformer" should "return correct Company Name (from PAYE record)" in {

    val expected = fullPayeRec.nameLine1.map(_.toUpperCase)
    val payeRecs = Some(Seq(fullPayeRec))
    val br = Business(100, None, None, payeRecs)
    val result = Transformers.getCompanyName(br)

    result should be(expected)
  }

  "A Transformer" should "return correct Postcode (from Company record)" in {

    val expected = fullCompanyRec.postcode

    val br = Business(100, Some(fullCompanyRec), None, None)
    val result = Transformers.getPostcode(br)

    result should be(expected)
  }

  "A Transformer" should "return correct Postcode (from VAT record)" in {

    val expected = fullVatRec.postcode
    val vatRecs = Some(Seq(fullVatRec))
    val br = Business(100, None, vatRecs, None)
    val result = Transformers.getPostcode(br)

    result should be(expected)

  }

  "A Transformer" should "return correct Postcode (from PAYE record)" in {

    val expected = fullPayeRec.postcode
    val payeRecs = Some(Seq(fullPayeRec))
    val br = Business(100, None, None, payeRecs)
    val result = Transformers.getPostcode(br)

    result should be(expected)
  }

  "A Transformer" should "convert PAYE jobs last updated (MMMYY) correctly to Date" in {

    val dt = Some(new DateTime("2016-06-01"))
    val jobsLastUpd = Some("Jun16")
    val expected = dt
    val result = Transformers.getLastUpdOpt(jobsLastUpd)

    result should be(expected)
  }

}
