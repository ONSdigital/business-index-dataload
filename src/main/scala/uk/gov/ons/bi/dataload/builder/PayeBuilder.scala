package uk.gov.ons.bi.dataload.builder

import uk.gov.ons.bi.dataload.model._

/**
  * Created by Volodymyr.Glushak on 09/02/2017.
  */
object PayeBuilder {

  def payeFromMap(map: Map[String, String]) = new PayeBuilder(map).build

}

class PayeBuilder(val map: Map[String, String]) extends RecordBuilder[PayeRecord2] {

  // entref,payeref,deathcode,birthdate,deathdate,mfullemp,msubemp,ffullemp,fsubemp,unclemp,unclsubemp,
  // dec_jobs,mar_jobs,june_jobs,sept_jobs,
  // jobs_lastupd,
  // legalstatus,prevpaye,
  // employer_cat,
  // stc,crn,
  // ctiondate,
  // addressref,
  // marker,
  // inqcode,
  // nameline1,nameline2,nameline3,
  // tradstyle1,tradstyle2,tradstyle3,
  // address1,address2,address3,address4,address5,
  // postcode

  def build = PayeRecord2(
    entref = map("entref"),
    payeref = map("payeref").toInt,
    deathcode = map("deathcode"),
    birthdate = map("birthdate"),
    deathdate = map("deathdate"),
    payeEmp = payeEmpFromMap,
    month_jobs = monthJobsFromMap,
    jobs_lastupd = map("jobs_lastupd"),
    legalstatus = map("legalstatus"),
    prevpaye = map("prevpaye"),
    employer_cat = map("employer_cat"),
    stc = map("stc"),
    crn = map("crn"),
    actiondate = map("actiondate"),
    addressref = map("addressref"),
    marker = map("marker"),
    inqcode = map("inqcode"),
    name = multiLineNameFromMap,
    tradstyle = tradStyleFromMap,
    address = addressFromMap
  )


  protected def payeEmpFromMap = PayeEmp(
    mfullemp = map("mfullemp").toInt,
    msubemp = map("msubemp").toInt,
    ffullemp = map("ffullemp").toInt,
    fsubemp = map("fsubemp").toInt,
    unclemp = map("unclemp").toInt,
    unclsubemp = map("unclsubemp").toInt
  )

  protected def monthJobsFromMap = MonthJobs(
    dec_jobs = map("dec_jobs"),
    mar_jobs = map("mar_jobs"),
    june_jobs = map("june_jobs"),
    sept_jobs = map("sept_jobs")
  )


}
