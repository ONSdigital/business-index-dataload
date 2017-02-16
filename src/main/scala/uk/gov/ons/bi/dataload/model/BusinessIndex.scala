package uk.gov.ons.bi.dataload.model

/**
  * Created by websc on 13/02/2017.
  */
// From Vlad's code
case class BusinessIndexES(
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
