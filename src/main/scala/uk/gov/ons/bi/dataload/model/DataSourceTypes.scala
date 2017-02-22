package uk.gov.ons.bi.dataload.model

/**
  * Created by websc on 10/02/2017.
  */
sealed trait BIDataSource

  case object CH extends BIDataSource

  case object VAT extends BIDataSource

  case object PAYE extends BIDataSource

  case object LINKS extends BIDataSource
