package uk.gov.ons.bi.dataload.utils

import org.joda.time.DateTime
import org.scalatest._
import uk.gov.ons.bi.dataload.model.{Business, CompanyRec, PayeRec, VatRec}

/**
  * Created by websc on 27/02/2017.
  */

class FieldTransformersFlatSpec extends FlatSpec with Matchers {

  val fullCompanyRec = CompanyRec(Some("Company1"), Some("CompanyOne"), Some("Status"), Some("SIC"), Some("Company Post Code"))

  val fullVatRec = VatRec(Some(100L), Some("VAT Name Line 1"), Some("VAT Post Code"),
    Some(92), Some(1), Some(12345))

  val fullPayeRec = PayeRec(Some("PAYE REF"), Some("PAYE Name Line 1"), Some("PAYE Post Code"),
    Some(2), Some(120.0D), Some(30.0D),
    Some(60.0D), Some(90.0D), Some("Jun16"))


  "A Transformer" should "get latest job figure from PAYE record" in {

    val dt = Some(new DateTime("2016-06-01"))
    val expected = (dt, Some(60.0D))
    val result: (Option[DateTime], Option[Double]) = Transformers.getLatestJobsForPayeRec(fullPayeRec)

    result should be(expected)
  }

  "A Transformer" should "return correct Industry Code (from Company record)" in {

    val expected = fullCompanyRec.sicCode1

    val br = Business("UBRN001", Some(fullCompanyRec), None, None)
    val result = Transformers.getIndustryCode(br)

    result should be(expected)
  }

  "A Transformer" should "return total VAT turnover" in {

    val expected = Option(fullVatRec.turnover.getOrElse(0L) + fullVatRec.turnover.getOrElse(0L))
    val vatRecs = Some(Seq(fullVatRec, fullVatRec))
    val br = Business("UBRN001", None, vatRecs, None)
    val result = Transformers.getVatTotalTurnover(br)

    result should be(expected)
  }

  "A Transformer" should "return correct Industry Code (from VAT record)" in {

    val expected = fullVatRec.sic92.map(_.toString)
    val vatRecs = Some(Seq(fullVatRec))
    val br = Business("UBRN001", None, vatRecs, None)
    val result = Transformers.getIndustryCode(br)

    result should be(expected)
  }

  "A Transformer" should "return all VAT references" in {

    val vat1 = fullVatRec
    val vat2 = fullVatRec.copy(vatRef = Some(98765L))
    val vatRecs = Seq(vat1, vat2)
    val expected: Seq[Long] = vatRecs.flatMap(_.vatRef)
    val br = Business("UBRN001", None, Some(vatRecs), None)
    val result: Option[Seq[Long]] = Transformers.getVatRefs(br)

    result should be(Option(expected))
  }

  "A Transformer" should "return all PAYE references" in {

    val rec1 = fullPayeRec
    val rec2 = fullPayeRec.copy(payeRef = Some("PAYE2"))
    val recs = Seq(rec1, rec2)
    val expected: Seq[String] = recs.flatMap(_.payeRef)
    val br = Business("UBRN001", None, None, Some(recs))
    val result: Option[Seq[String]] = Transformers.getPayeRefs(br)

    result should be(Option(expected))
  }

  "A Transformer" should "return correct Legal Status (from VAT record)" in {

    val expected = fullVatRec.legalStatus.map(_.toString)
    val vatRecs = Some(Seq(fullVatRec))
    val br = Business("UBRN001", None, vatRecs, None)
    val result = Transformers.getLegalStatus(br)

    result should be(expected)
  }

  "A Transformer" should "return correct Legal Status (from PAYE record)" in {

    val expected = fullPayeRec.legalStatus.map(_.toString)
    val payeRecs = Some(Seq(fullPayeRec))
    val br = Business("UBRN001", None, None, payeRecs)
    val result = Transformers.getLegalStatus(br)

    result should be(expected)
  }

  "A Transformer" should "return correct Company Name (from Company record)" in {

    val expected = fullCompanyRec.companyName

    val br = Business("UBRN001", Some(fullCompanyRec), None, None)
    val result = Transformers.getCompanyName(br)

    result should be(expected)
  }


  "A Transformer" should "return correct Company Name (from VAT record)" in {

    val expected = fullVatRec.nameLine1
    val vatRecs = Some(Seq(fullVatRec))
    val br = Business("UBRN001", None, vatRecs, None)
    val result = Transformers.getCompanyName(br)

    result should be(expected)
  }

  "A Transformer" should "return correct Company Name (from PAYE record)" in {

    val expected = fullPayeRec.nameLine1
    val payeRecs = Some(Seq(fullPayeRec))
    val br = Business("UBRN001", None, None, payeRecs)
    val result = Transformers.getCompanyName(br)

    result should be(expected)
  }

  "A Transformer" should "return correct Postcode (from Company record)" in {

    val expected = fullCompanyRec.postcode

    val br = Business("UBRN001", Some(fullCompanyRec), None, None)
    val result = Transformers.getPostcode(br)

    result should be(expected)
  }

  "A Transformer" should "return correct Postcode (from VAT record)" in {

    val expected = fullVatRec.postcode
    val vatRecs = Some(Seq(fullVatRec))
    val br = Business("UBRN001", None, vatRecs, None)
    val result = Transformers.getPostcode(br)

    result should be(expected)

  }

  "A Transformer" should "return correct Postcode (from PAYE record)" in {

    val expected = fullPayeRec.postcode
    val payeRecs = Some(Seq(fullPayeRec))
    val br = Business("UBRN001", None, None, payeRecs)
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
