package uk.gov.ons.bi.dataload.utils

import org.scalatest.{FlatSpec, ShouldMatchers}
import uk.gov.ons.bi.dataload.model._


/**
  * Created by websc on 31/03/2017.
  */
class RecordTransformersFlatSpec extends FlatSpec with ShouldMatchers {

  behavior of "RecordTransformersFlatSpec"

  "A Transformer" should "explodeLink() from a complete LinkRec to a Seq[UbrnWithKey]" in {

    val ch1 = "CH1"
    val vat1 = "VAT1"
    val vat2 = "VAT2"
    val paye1 = "PAYE1"
    val paye2 = "PAYE2"
    val link = LinkRec(1L, Some(ch1), Some(List(vat1, vat2)), Some(List(paye1, paye2)))

    val result: Seq[UbrnWithKey] = Transformers.explodeLink(link)

    val expected: Seq[UbrnWithKey] = List(
      UbrnWithKey(1, CH, ch1),
      UbrnWithKey(1, VAT, vat1),
      UbrnWithKey(1, VAT, vat2),
      UbrnWithKey(1, PAYE, paye1),
      UbrnWithKey(1, PAYE, paye2))

    result shouldBe (expected)
  }

  "A Transformer" should "explodeLink() with missing CH correctly to a Seq[UbrnWithKey]" in {

    val ch1 = "CH1"
    val vat1 = "VAT1"
    val vat2 = "VAT2"
    val paye1 = "PAYE1"
    val paye2 = "PAYE2"
    val link = LinkRec(1L, None, Some(List(vat1, vat2)), Some(List(paye1, paye2)))

    val result: Seq[UbrnWithKey] = Transformers.explodeLink(link)

    val expected: Seq[UbrnWithKey] = List(
      UbrnWithKey(1, VAT, vat1),
      UbrnWithKey(1, VAT, vat2),
      UbrnWithKey(1, PAYE, paye1),
      UbrnWithKey(1, PAYE, paye2))

    result shouldBe (expected)
  }


  "A Transformer" should "explodeLink() with missing VAT correctly to a Seq[UbrnWithKey]" in {

    val ch1 = "CH1"
    val vat1 = "VAT1"
    val vat2 = "VAT2"
    val paye1 = "PAYE1"
    val paye2 = "PAYE2"
    val link = LinkRec(1L, Some(ch1), None, Some(List(paye1, paye2)))

    val result: Seq[UbrnWithKey] = Transformers.explodeLink(link)

    val expected: Seq[UbrnWithKey] = List(
      UbrnWithKey(1, CH, ch1),
      UbrnWithKey(1, PAYE, paye1),
      UbrnWithKey(1, PAYE, paye2))

    result shouldBe (expected)
  }

  "A Transformer" should "explodeLink() with missing PAYE correctly to a Seq[UbrnWithKey]" in {

    val ch1 = "CH1"
    val vat1 = "VAT1"
    val vat2 = "VAT2"
    val paye1 = "PAYE1"
    val paye2 = "PAYE2"
    val link = LinkRec(1L, Some(ch1), Some(List(vat1, vat2)), None)

    val result: Seq[UbrnWithKey] = Transformers.explodeLink(link)

    val expected: Seq[UbrnWithKey] = List(
      UbrnWithKey(1, CH, ch1),
      UbrnWithKey(1, VAT, vat1),
      UbrnWithKey(1, VAT, vat2))

    result shouldBe (expected)
  }

  "A Transformer" should "explodeLink() with empty PAYE list correctly to a Seq[UbrnWithKey]" in {

    val ch1 = "CH1"
    val vat1 = "VAT1"
    val vat2 = "VAT2"
    val paye1 = "PAYE1"
    val paye2 = "PAYE2"
    val link = LinkRec(1L, Some(ch1), Some(List(vat1, vat2)), Some(Nil))

    val result: Seq[UbrnWithKey] = Transformers.explodeLink(link)

    val expected: Seq[UbrnWithKey] = List(
      UbrnWithKey(1, CH, ch1),
      UbrnWithKey(1, VAT, vat1),
      UbrnWithKey(1, VAT, vat2))

    result shouldBe (expected)
  }

  "A Transformer" should "explodeLink() with empty VAT List correctly to a Seq[UbrnWithKey]" in {

    val ch1 = "CH1"
    val vat1 = "VAT1"
    val vat2 = "VAT2"
    val paye1 = "PAYE1"
    val paye2 = "PAYE2"
    val link = LinkRec(1L, Some(ch1), Some(Nil), Some(List(paye1, paye2)))

    val result: Seq[UbrnWithKey] = Transformers.explodeLink(link)

    val expected: Seq[UbrnWithKey] = List(
      UbrnWithKey(1, CH, ch1),
      UbrnWithKey(1, PAYE, paye1),
      UbrnWithKey(1, PAYE, paye2))

    result shouldBe (expected)
  }


  "A Transformer" should "buildBusinessRecord from UbrnWithList to Business record" in {

    // Set up source data for a Business with 1 company, 2 VAT, 2 PAYE
    val ubrn = 100L
    val company = CompanyRec(companyNo = Some("CH1"), companyName = Some("TEST CH1"),
      companyStatus = Some("Status"), sicCode1 = Some("SIC"), postcode = Some("AB1 2CD")
    )
    val uwdCh = UbrnWithData(ubrn, CH, company)

    val vat1 = VatRec(vatRef = Some(1L), nameLine1 = Some("TEST VAT1"), postcode = Some("AB1 2CD"),
      sic92 = Some("9"), legalStatus = Some(2), turnover = Some(12345L))
    val uwdVat1 = UbrnWithData(ubrn, VAT, vat1)

    val vat2 = VatRec(vatRef = Some(1L), nameLine1 = Some("TEST VAT2"), postcode = Some("AB1 2CD"),
      sic92 = Some("9"), legalStatus = Some(2), turnover = Some(12345L))
    val uwdVat2 = UbrnWithData(ubrn, VAT, vat2)

    val paye1 = PayeRec(payeRef = Some("PAYE1"), nameLine1 = Some("TEST PAYE1"), postcode = Some("AB1 2CD"),
      legalStatus = Some(3), decJobs = Some(12.0), marJobs = Some(3.0),
      junJobs = Some(6.0), sepJobs = Some(9.0), jobsLastUpd = Some("Mar17"))
    val uwdPaye1 = UbrnWithData(ubrn, PAYE, paye1)

    val paye2 = PayeRec(payeRef = Some("PAYE1"), nameLine1 = Some("TEST PAYE2"), postcode = Some("AB1 2CD"),
      legalStatus = Some(3), decJobs = Some(12.0), marJobs = Some(3.0),
      junJobs = Some(6.0), sepJobs = Some(9.0), jobsLastUpd = Some("Jun16"))
    val uwdPaye2 = UbrnWithData(ubrn, PAYE, paye2)

    val uwds = List(uwdCh, uwdVat1, uwdVat2, uwdPaye1, uwdPaye2)
    val uwl = UbrnWithList(ubrn, uwds)

    // Results?

    val results: Business = Transformers.buildBusinessRecord(uwl)
    val expected = Business(ubrn, Some(company), Some(List(vat1, vat2)), Some(List(paye1, paye2)))

    results shouldBe (expected)
  }

  "A Transformer" should "buildBusinessRecord from UbrnWithList with no Company correctly" in {

    // Set up source data for a Business with  2 VAT, 2 PAYE
    val ubrn = 100L

    val vat1 = VatRec(vatRef = Some(1L), nameLine1 = Some("TEST VAT1"), postcode = Some("AB1 2CD"),
      sic92 = Some("9"), legalStatus = Some(2), turnover = Some(12345L))
    val uwdVat1 = UbrnWithData(ubrn, VAT, vat1)

    val vat2 = VatRec(vatRef = Some(1L), nameLine1 = Some("TEST VAT2"), postcode = Some("AB1 2CD"),
      sic92 = Some("9"), legalStatus = Some(2), turnover = Some(12345L))
    val uwdVat2 = UbrnWithData(ubrn, VAT, vat2)

    val paye1 = PayeRec(payeRef = Some("PAYE1"), nameLine1 = Some("TEST PAYE1"), postcode = Some("AB1 2CD"),
      legalStatus = Some(3), decJobs = Some(12.0), marJobs = Some(3.0),
      junJobs = Some(6.0), sepJobs = Some(9.0), jobsLastUpd = Some("Mar17"))
    val uwdPaye1 = UbrnWithData(ubrn, PAYE, paye1)

    val paye2 = PayeRec(payeRef = Some("PAYE1"), nameLine1 = Some("TEST PAYE2"), postcode = Some("AB1 2CD"),
      legalStatus = Some(3), decJobs = Some(12.0), marJobs = Some(3.0),
      junJobs = Some(6.0), sepJobs = Some(9.0), jobsLastUpd = Some("Jun16"))
    val uwdPaye2 = UbrnWithData(ubrn, PAYE, paye2)

    val uwds = List(uwdVat1, uwdVat2, uwdPaye1, uwdPaye2)
    val uwl = UbrnWithList(ubrn, uwds)

    // Results?

    val results: Business = Transformers.buildBusinessRecord(uwl)
    val expected = Business(ubrn, None, Some(List(vat1, vat2)), Some(List(paye1, paye2)))

    results shouldBe (expected)
  }

  "A Transformer" should "buildBusinessRecord from UbrnWithList with no VAT correctly" in {

    // Set up source data for a Business with 1 company, no VAT, 2 PAYE
    val ubrn = 100L
    val company = CompanyRec(companyNo = Some("CH1"), companyName = Some("TEST CH1"),
      companyStatus = Some("Status"), sicCode1 = Some("SIC"), postcode = Some("AB1 2CD")
    )
    val uwdCh = UbrnWithData(ubrn, CH, company)

    val paye1 = PayeRec(payeRef = Some("PAYE1"), nameLine1 = Some("TEST PAYE1"), postcode = Some("AB1 2CD"),
      legalStatus = Some(3), decJobs = Some(12.0), marJobs = Some(3.0),
      junJobs = Some(6.0), sepJobs = Some(9.0), jobsLastUpd = Some("Mar17"))
    val uwdPaye1 = UbrnWithData(ubrn, PAYE, paye1)

    val paye2 = PayeRec(payeRef = Some("PAYE1"), nameLine1 = Some("TEST PAYE2"), postcode = Some("AB1 2CD"),
      legalStatus = Some(3), decJobs = Some(12.0), marJobs = Some(3.0),
      junJobs = Some(6.0), sepJobs = Some(9.0), jobsLastUpd = Some("Jun16"))
    val uwdPaye2 = UbrnWithData(ubrn, PAYE, paye2)

    val uwds = List(uwdCh, uwdPaye1, uwdPaye2)
    val uwl = UbrnWithList(ubrn, uwds)

    // Results?

    val results: Business = Transformers.buildBusinessRecord(uwl)
    val expected = Business(ubrn, Some(company), None, Some(List(paye1, paye2)))

    results shouldBe (expected)
  }


  "A Transformer" should "buildBusinessRecord from UbrnWithList with no PAYE correctly" in {

    // Set up source data for a Business with 1 company, 2 VAT, no PAYE
    val ubrn = 100L
    val company = CompanyRec(companyNo = Some("CH1"), companyName = Some("TEST CH1"),
      companyStatus = Some("Status"), sicCode1 = Some("SIC"), postcode = Some("AB1 2CD")
    )
    val uwdCh = UbrnWithData(ubrn, CH, company)

    val vat1 = VatRec(vatRef = Some(1L), nameLine1 = Some("TEST VAT1"), postcode = Some("AB1 2CD"),
      sic92 = Some("9"), legalStatus = Some(2), turnover = Some(12345L))
    val uwdVat1 = UbrnWithData(ubrn, VAT, vat1)

    val vat2 = VatRec(vatRef = Some(1L), nameLine1 = Some("TEST VAT2"), postcode = Some("AB1 2CD"),
      sic92 = Some("9"), legalStatus = Some(2), turnover = Some(12345L))
    val uwdVat2 = UbrnWithData(ubrn, VAT, vat2)

    val uwds = List(uwdCh, uwdVat1, uwdVat2)
    val uwl = UbrnWithList(ubrn, uwds)

    // Results?

    val results: Business = Transformers.buildBusinessRecord(uwl)
    val expected = Business(ubrn, Some(company), Some(List(vat1, vat2)), None)

    results shouldBe (expected)
  }

  "A Transformer" should "extractUwlVats from UbrnWithList with VAT records to VatRecs correctly" in {

    val ubrn = 100L

    val vat1 = VatRec(vatRef = Some(1L), nameLine1 = Some("TEST VAT1"), postcode = Some("AB1 2CD"),
      sic92 = Some("9"), legalStatus = Some(2), turnover = Some(12345L))
    val uwdVat1 = UbrnWithData(ubrn, VAT, vat1)

    val vat2 = VatRec(vatRef = Some(1L), nameLine1 = Some("TEST VAT2"), postcode = Some("AB1 2CD"),
      sic92 = Some("9"), legalStatus = Some(2), turnover = Some(12345L))
    val uwdVat2 = UbrnWithData(ubrn, VAT, vat2)

    val uwds = List(uwdVat1, uwdVat2)
    val uwl = UbrnWithList(ubrn, uwds)

    val results = Transformers.extractUwlVats(uwl)
    val expected = Option(List(vat1, vat2))

    results shouldBe expected
  }

  "A Transformer" should "extractUwlVats from UbrnWithList with NO VAT records correctly" in {

    val ubrn = 100L

    val company = CompanyRec(companyNo = Some("CH1"), companyName = Some("TEST CH1"),
      companyStatus = Some("Status"), sicCode1 = Some("SIC"), postcode = Some("AB1 2CD")
    )
    val uwdCh = UbrnWithData(ubrn, CH, company)

    val uwds = List(uwdCh)
    val uwl = UbrnWithList(ubrn, uwds)

    val results = Transformers.extractUwlVats(uwl)
    val expected = None

    results shouldBe expected
  }

  "A Transformer" should "extractUwlPayes from UbrnWithList with PAYE records to PAYE recs correctly" in {

    val ubrn = 100L

    val paye1 = PayeRec(payeRef = Some("PAYE1"), nameLine1 = Some("TEST PAYE1"), postcode = Some("AB1 2CD"),
      legalStatus = Some(3), decJobs = Some(12.0), marJobs = Some(3.0),
      junJobs = Some(6.0), sepJobs = Some(9.0), jobsLastUpd = Some("Mar17"))
    val uwdPaye1 = UbrnWithData(ubrn, PAYE, paye1)

    val paye2 = PayeRec(payeRef = Some("PAYE1"), nameLine1 = Some("TEST PAYE2"), postcode = Some("AB1 2CD"),
      legalStatus = Some(3), decJobs = Some(12.0), marJobs = Some(3.0),
      junJobs = Some(6.0), sepJobs = Some(9.0), jobsLastUpd = Some("Jun16"))
    val uwdPaye2 = UbrnWithData(ubrn, PAYE, paye2)

    val uwds = List(uwdPaye1, uwdPaye2)
    val uwl = UbrnWithList(ubrn, uwds)

    val results = Transformers.extractUwlPayes(uwl)
    val expected = Option(List(paye1, paye2))

    results shouldBe expected
  }

  "A Transformer" should "extractUwlPayes from UbrnWithList with NO PAYE records correctly" in {

    val ubrn = 100L

    val company = CompanyRec(companyNo = Some("CH1"), companyName = Some("TEST CH1"),
      companyStatus = Some("Status"), sicCode1 = Some("SIC"), postcode = Some("AB1 2CD")
    )
    val uwdCh = UbrnWithData(ubrn, CH, company)

    val uwds = List(uwdCh)
    val uwl = UbrnWithList(ubrn, uwds)

    val results = Transformers.extractUwlPayes(uwl)
    val expected = None

    results shouldBe expected
  }

  "A Transformer" should "convertToBusinessIndex correctly for full set of business data" in {

    // We assume the individual fields are tested elsewhere

    // Set up source data for a Business with 1 company, 2 VAT, 2 PAYE,
    // Company SIC = "123 SIC" (should return IndustryCode = 123)
    val ubrn = 100L
    val company = CompanyRec(companyNo = Some("CH1"), companyName = Some("TEST CH1"),
      companyStatus = Some("Active"), sicCode1 = Some("123 SIC"), postcode = Some("AB1 2CD")
    )
    val uwdCh = UbrnWithData(ubrn, CH, company)

    val vat1 = VatRec(vatRef = Some(1L), nameLine1 = Some("TEST VAT1"), postcode = Some("AB1 2CD"),
      sic92 = Some("9"), legalStatus = Some(2), turnover = Some(12345L))
    val uwdVat1 = UbrnWithData(ubrn, VAT, vat1)

    val vat2 = VatRec(vatRef = Some(1L), nameLine1 = Some("TEST VAT2"), postcode = Some("AB1 2CD"),
      sic92 = Some("9"), legalStatus = Some(2), turnover = Some(12345L))
    val uwdVat2 = UbrnWithData(ubrn, VAT, vat2)

    val paye1 = PayeRec(payeRef = Some("PAYE1"), nameLine1 = Some("TEST PAYE1"), postcode = Some("AB1 2CD"),
      legalStatus = Some(3), decJobs = Some(12.0), marJobs = Some(3.0),
      junJobs = Some(6.0), sepJobs = Some(9.0), jobsLastUpd = Some("Mar17"))
    val uwdPaye1 = UbrnWithData(ubrn, PAYE, paye1)

    val paye2 = PayeRec(payeRef = Some("PAYE1"), nameLine1 = Some("TEST PAYE2"), postcode = Some("AB1 2CD"),
      legalStatus = Some(3), decJobs = Some(12.0), marJobs = Some(3.0),
      junJobs = Some(6.0), sepJobs = Some(9.0), jobsLastUpd = Some("Jun16"))
    val uwdPaye2 = UbrnWithData(ubrn, PAYE, paye2)

    val uwds = List(uwdCh, uwdVat1, uwdVat2, uwdPaye1, uwdPaye2)
    val uwl = UbrnWithList(ubrn, uwds)

    // Make a full Business record
    val business: Business = Transformers.buildBusinessRecord(uwl)

    // Run conversion
    val results = Transformers.convertToBusinessIndex(business)

    val expectedVatRefs = Some(List(vat1.vatRef.get, vat2.vatRef.get))
    val expectedPayeRefs = Some(List(paye1.payeRef.get, paye2.payeRef.get))

    val expected = BusinessIndex(ubrn, company.companyName, company.postcode, Some("123"),
      Some("1"),  // default legal status for Company
      Some("A"),  // trading status
      Some("H"),
      Some("C"),
      company.companyNo,
      expectedVatRefs,
      expectedPayeRefs
    )
    results shouldBe expected
  }

  "A Transformer" should "convertToBusinessIndex with correct legal status if only Company is present" in {

    // We assume the individual fields are tested elsewhere
    // Set up source data for a Business with 1 company only
    val ubrn = 100L
    val company = CompanyRec(companyNo = Some("CH1"), companyName = Some("TEST CH1"),
      companyStatus = Some("Status"), sicCode1 = Some("123 SIC"), postcode = Some("AB1 2CD")
    )
    val uwdCh = UbrnWithData(ubrn, CH, company)

    val uwds = List(uwdCh)
    val uwl = UbrnWithList(ubrn, uwds)

    // Construct a full Business record
    val business: Business = Transformers.buildBusinessRecord(uwl)

    // Run conversion
    val results  = Transformers.convertToBusinessIndex(business).legalStatus
    val expected = Some("1") // legalStatus

    results shouldBe expected
  }

}
