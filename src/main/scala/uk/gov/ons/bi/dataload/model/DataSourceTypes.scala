package uk.gov.ons.bi.dataload.model

/**
  * Created by websc on 10/02/2017.
  */
sealed trait BIDataSource

  case object CH extends BIDataSource {override val toString = "CH"}

  case object VAT extends BIDataSource {override val toString = "VAT"}

  case object PAYE extends BIDataSource {override val toString = "PAYE"}

  case object LINKS extends BIDataSource {override val toString = "LINKS"}

