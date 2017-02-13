package uk.gov.ons.bi.dataload.builder

import uk.gov.ons.bi.dataload.model.VatRecord

/**
  * Created by Volodymyr.Glushak on 09/02/2017.
  */
object VATBuilder {

  def vatFromMap(map: Map[String, String]) = new VATBuilder(map).build

}

class VATBuilder(val map: Map[String, String]) extends RecordBuilder[VatRecord] {
  override def build: VatRecord = VatRecord(
    entref = map("entref"),
    vatref = map("vatref"),
    deathcode = map("deathcode"),
    birthdate = map("birthdate"),
    deathdate = map("deathdate"),
    sic92 = map("sic92"),
    turnover = map("turnover"),
    turnover_date = map("turnover_date"),
    record_type = map("record_type"),
    legalstatus = map("legalstatus"),
    actiondate = map("actiondate"),
    crn = map("crn"),
    marker = map("marker"),
    addressref = map("addressref"),
    inqcode = map("inqcode")
    /*,
    name = multiLineNameFromMap,
    tradStyle = tradStyleFromMap,
    address = addressFromMap
*/  )
}
