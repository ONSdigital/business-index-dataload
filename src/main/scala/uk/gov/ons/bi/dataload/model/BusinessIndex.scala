package uk.gov.ons.bi.dataload.model

/**
  * Created by websc on 13/02/2017.
  */
// fixme: the same class exists in bi-api
case class BusinessIndex(
                          id: Long,
                          name: String,
                          uprn: Long,
                          industryCode: Long,
                          legalStatus: String,
                          tradingStatus: String,
                          turnover: String,
                          employmentBand: String
                        ) {

  val Delim = ","
  def toCsv =
    s"""$id $Delim "$name" $Delim $uprn $Delim $industryCode $Delim $legalStatus $Delim "$tradingStatus" $Delim "$turnover" $Delim "$employmentBand" """

}

