package uk.gov.ons.bi.dataload.model

/**
  * Created by websc on 03/03/2017.
  */


object BiTypes {
  // UBRN is currently defined as a Long number, but this may change, so use a synonym
  // for the data-type to make it easier to change all the references later.
  type Ubrn = Long
}

// old Link format from data science
case class LinkRec(ubrn: BiTypes.Ubrn, ch: Option[String], vat: Option[Seq[String]], paye: Option[Seq[String]])

// link keys for CH/PAYE/VAT
case class LinkKeys(ch: Option[String], vat: Option[Seq[String]], paye: Option[Seq[String]])

// revised Link format allows for no UBRN from data science and encapsulates link key data
case class Link(ubrn: Option[BiTypes.Ubrn] = None, link: LinkKeys)
