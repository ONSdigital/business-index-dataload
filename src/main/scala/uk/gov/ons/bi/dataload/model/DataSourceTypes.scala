package uk.gov.ons.bi.dataload.model

/**
  * Created by websc on 10/02/2017.
  */

// These are used to tag our various intermediate objects with the relevant data source type
// during processing. In some cases we want to be able to treat them all the same, in others
// we want to distinguish between them.  These traits allow us to do this.

trait BIDataSource

sealed trait BusinessDataSource extends BIDataSource

  case object CH extends BusinessDataSource

  case object VAT extends BusinessDataSource

  case object PAYE extends BusinessDataSource

  case object TCN extends  BusinessDataSource


sealed trait OnsDataSource extends BIDataSource

  case object LINKS extends OnsDataSource

  case object TCN_SIC_LOOKUP extends OnsDataSource
