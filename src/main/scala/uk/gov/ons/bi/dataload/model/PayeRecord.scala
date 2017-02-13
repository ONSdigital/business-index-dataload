package uk.gov.ons.bi.dataload.model


case class Address(
  line_1: String,
  line_2: String,
  line_3: String,
  line_4: String,
  line_5: String,
  postcode: String
)

case class PayeName(nameline1: String, nameline2: String, nameline3: String) {
  override def toString: String = s"$nameline1 $nameline2 $nameline3"
}

case class TradStyle(tradstyle1: String, tradstyle2: String, tradstyle3: String)

case class MonthJobs(dec_jobs: String, mar_jobs: String, june_jobs: String, sept_jobs: String)

case class PayeEmp(
  mfullemp: Int,
  msubemp: Int,
  ffullemp: Int,
  fsubemp: Int,
  unclemp: Int,
  unclsubemp: Int
)

case class PayeRecord2(
  entref: String,
  payeref: Int,
  deathcode: String,
  birthdate: String,
  deathdate: String,
  payeEmp: PayeEmp,
  month_jobs: MonthJobs,
  jobs_lastupd: String,
  legalstatus: String,
  prevpaye: String,
  employer_cat: String,
  stc: String,
  crn: String,
  actiondate: String,
  addressref: String,
  marker: String,
  inqcode: String,
  name: PayeName,
  tradstyle: TradStyle,
  address: Address
)

