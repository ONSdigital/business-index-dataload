package uk.gov.ons.bi.dataload.model

/**
  * Created by websc on 10/02/2017.
  */


trait BIDataSource

sealed trait BusinessDataSource extends BIDataSource

  case object CH extends BusinessDataSource

  case object VAT extends BusinessDataSource

  case object PAYE extends BusinessDataSource


sealed trait LinksData extends BIDataSource

  case object LINKS extends LinksData
