package uk.gov.ons.bi.dataload.utils

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

  def legalStatusBand(s: Option[String]): Option[Int] = s map {
    case "Company" => 1
    case "Sole Proprietor" => 2
    case "Partnership" => 3
    case "Public Corporation" => 4
    case "Non-Profit Organisation" => 5
    case "Local Authority" => 6
    case "Central Government" => 7
    case "Charity" => 8
    case _ => 0
  }

  def tradingStatusBand(s: Option[String]): Option[String] = s map {
    case "Active" => "A"
    case "Closed" => "C"
    case "Dormant" => "D"
    case "Insolvent" => "I"
    case _ => "?"
  }

}