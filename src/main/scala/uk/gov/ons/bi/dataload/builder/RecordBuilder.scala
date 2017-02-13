package uk.gov.ons.bi.dataload.builder

import uk.gov.ons.bi.dataload.model.{Address, PayeName, TradStyle}

/**
  * Created by Volodymyr.Glushak on 09/02/2017.
  */
trait RecordBuilder[T] {

  def map: Map[String, String]

  def build: T


  // util methods below that are used in more than one type of records

  protected def multiLineNameFromMap = PayeName(
    nameline1 = map("nameline1"),
    nameline2 = map("nameline2"),
    nameline3 = map("nameline3")
  )

  protected def tradStyleFromMap = TradStyle(
    tradstyle1 = map("tradstyle1"),
    tradstyle2 = map("tradstyle2"),
    tradstyle3 = map("tradstyle3")
  )

  protected def addressFromMap = Address(
    line_1 = map("address1"),
    line_2 = map("address2"),
    line_3 = map("address3"),
    line_4 = map("address4"),
    line_5 = map("address5"),
    postcode = map("postcode")
  )

}
