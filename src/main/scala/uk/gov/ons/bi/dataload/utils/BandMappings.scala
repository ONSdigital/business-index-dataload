package uk.gov.ons.bi.dataload.utils

import uk.gov.ons.bi.dataload.utils.BandMappings.TradingStatus

/**
  * Created by websc on 22/02/2017.
  */


object BandMappings {

  def employmentBand(z: Option[Int]): Option[String] =
    z map {
      case 0 => "A"
      case 1 => "B"
      case x if x < 5 => "C"
      case x if x < 10 => "D"
      case x if x < 20 => "E"
      case x if x < 25 => "F"
      case x if x < 50 => "G"
      case x if x < 75 => "H"
      case x if x < 100 => "I"
      case x if x < 150 => "J"
      case x if x < 200 => "K"
      case x if x < 250 => "L"
      case x if x < 300 => "M"
      case x if x < 500 => "N"
      case _ => "O"
    }


  def turnoverBand(z: Option[Long]): Option[String] = z map {
    case x if x < 100 => "A"
    case x if x < 250 => "B"
    case x if x < 500 => "C"
    case x if x < 1000 => "D"
    case x if x < 2000 => "E"
    case x if x < 5000 => "F"
    case x if x < 10000 => "G"
    case x if x < 50000 => "H"
    case _ => "I"
  }


  object TradingStatus  {
    val ACTIVE = "Active"
    val CLOSED = "Closed"
    val DORMANT = "Dormant"
    val INSOLVENT = "Insolvent"
    val UNKNOWN = "?"
  }

  def tradingStatusToBand(s: Option[String]): Option[String] = {
    // ONSRBIB-570: invalid code should now translate as None
    s map {
      case TradingStatus.ACTIVE => "A"
      case TradingStatus.CLOSED => "C"
      case TradingStatus.DORMANT => "D"
      case TradingStatus.INSOLVENT => "I"
      case _ => "?"
    } match {
      case Some("?") => None
      case x => x}
  }

  def deathCodeTradingStatus(s: Option[String]): Option[String] = {
    // converts VAT / PAYE deathcode to trading status
    s map {
      case "0" | "2" | "4" | "6" | "7" | "8" | "E" | "M" | "S" | "T" => TradingStatus.ACTIVE
      case "1" | "9" => TradingStatus.CLOSED
      case "3" => TradingStatus.INSOLVENT
      case "5" => TradingStatus.DORMANT
      case _ => TradingStatus.UNKNOWN
    } match {
      case Some("?") => None
      case x => x}
  }
}