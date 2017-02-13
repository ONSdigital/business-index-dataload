package uk.gov.ons.bi.dataload.model

import org.joda.time.DateTime


case class Accounts(
  accounts_ref_day: String,
  accounts_ref_month: String,
  next_due_date: Option[DateTime],
  last_made_up_date: Option[DateTime],
  account_category: Option[String]
)

case class Returns(
  next_due_date: DateTime,
  last_made_up_date: Option[DateTime]
)

case class Mortgages(
  num_mort_charges: Option[Int],
  num_mort_outstanding: Option[Int],
  num_mort_part_satisfied: Option[Int],
  num_mort_satisfied: Option[Int]
)

case class SICCode(
  sic_text_1: String,
  sic_text_2: String,
  sic_text_3: String,
  sic_text_4: String
)

case class LimitedPartnerships(
  num_gen_partners: Int,
  num_lim_partners: Int
)

case class PreviousName(
  condate: String,
  company_name: String
)

case class RegistrationAddress(
  care_of: Option[String],
  po_box: Option[String],
  address_line_1: String,
  address_line_2: String,
  post_town: Option[String],
  county: Option[String],
  country: Option[String],
  postcode: Option[String]
)

case class PreviousNames(
  previous_name_1: Option[PreviousName],
  previous_name_2: Option[PreviousName],
  previous_name_3: Option[PreviousName],
  previous_name_4: Option[PreviousName],
  previous_name_5: Option[PreviousName],
  previous_name_6: Option[PreviousName],
  previous_name_7: Option[PreviousName],
  previous_name_8: Option[PreviousName],
  previous_name_9: Option[PreviousName],
  previous_name_10: Option[PreviousName]
)

case class CompaniesHouseRecord(
  id: String,
  company_name: String,
  company_number: String,
  company_category: String,
  company_status: String,
  country_of_origin: String,
  dissolution_date: Option[DateTime],
  incorporation_date: Option[DateTime],
  accounts: Accounts,
  returns: Option[Returns],
  sic_code: Option[SICCode],
  limitedPartnerships: LimitedPartnerships,
  uri: Option[String],
  previous_names: PreviousNames

)
