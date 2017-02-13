package uk.gov.ons.bi.dataload.builder

import uk.gov.ons.bi.dataload.model._


import uk.gov.ons.bi.dataload.parsers.ImplicitHelpers._

/**
  * Created by Volodymyr.Glushak on 08/02/2017.
  */

object CHBuilder {
  def companyHouseFromMap(map: Map[String, String]) = new CHBuilder(map).build
}

class CHBuilder(val map: Map[String, String]) extends RecordBuilder[CompaniesHouseRecord] {

  // CompanyName,CompanyNumber,
  // RegAddressCareOf,RegAddressPOBox,
  // RegAddressAddressLine1,RegAddressAddressLine2,RegAddressPostTown,RegAddressCounty,RegAddressCountry,RegAddressPostCode,
  // CompanyCategory,CompanyStatus,CountryOfOrigin,
  // DissolutionDate,IncorporationDate,
  // AccountsAccountRefDay,AccountsAccountRefMonth,AccountsNextDueDate,AccountsLastMadeUpDate,AccountsAccountCategory,
  // ReturnsNextDueDate,ReturnsLastMadeUpDate,
  // MortgagesNumMortCharges,MortgagesNumMortOutstanding,MortgagesNumMortPartSatisfied,MortgagesNumMortSatisfied,
  // SICCodeSicText_1,SICCodeSicText_2,SICCodeSicText_3,SICCodeSicText_4,
  // LimitedPartnershipsNumGenPartners,LimitedPartnershipsNumLimPartners,
  // URI,
  // PreviousName_1CONDATE,PreviousName_1CompanyName,
  // PreviousName_2CONDATE,PreviousName_2CompanyName,
  // PreviousName_3CONDATE,PreviousName_3CompanyName,
  // PreviousName_4CONDATE,PreviousName_4CompanyName,
  // PreviousName_5CONDATE,PreviousName_5CompanyName,
  // PreviousName_6CONDATE,PreviousName_6CompanyName,
  // PreviousName_7CONDATE,PreviousName_7CompanyName,
  // PreviousName_8CONDATE,PreviousName_8CompanyName,
  // PreviousName_9CONDATE,PreviousName_9CompanyName,
  // PreviousName_10CONDATE,PreviousName_10CompanyName


  def build = CompaniesHouseRecord(
      id = "???",
      company_name = map("CompanyName"),
      company_number = map("CompanyNumber"),
      company_category = map("CompanyCategory"),
      company_status = map("CompanyStatus"),
      country_of_origin = map("CountryOfOrigin"),
      dissolution_date = map("DissolutionDate").asDateTimeOpt,
      incorporation_date = map("IncorporationDate").asDateTimeOpt,
      accounts = accountFromMap,
      returns = returnsFromMap.?,
      sic_code = sicCodeFromMap.?,
      limitedPartnerships = limitedPartnershipFromMap,
      uri = map("URI").?,
      previous_names = previousNamesFromMap
    )

  // AccountsAccountRefDay,AccountsAccountRefMonth,AccountsNextDueDate,AccountsLastMadeUpDate,AccountsAccountCategory,

  def accountFromMap = {
    Accounts(
      accounts_ref_day = map("AccountsAccountRefDay"),
      accounts_ref_month = map("AccountsAccountRefMonth"),
      next_due_date = map("AccountsNextDueDate").asDateTimeOpt,
      last_made_up_date = map("AccountsLastMadeUpDate").asDateTimeOpt,
      account_category = map("AccountsAccountCategory").?
    )
  }

  // ReturnsNextDueDate,ReturnsLastMadeUpDate,
  def returnsFromMap = {
    Returns(
      next_due_date = map("ReturnsNextDueDate").asDateTime,
      last_made_up_date = map("ReturnsLastMadeUpDate").asDateTimeOpt
    )
  }

  // SICCodeSicText_1,SICCodeSicText_2,SICCodeSicText_3,SICCodeSicText_4,
  def sicCodeFromMap = {
    SICCode(
      sic_text_1 = map("SICCodeSicText_1"),
      sic_text_2 = map("SICCodeSicText_2"),
      sic_text_3 = map("SICCodeSicText_3"),
      sic_text_4 = map("SICCodeSicText_4")
    )
  }


  // LimitedPartnershipsNumGenPartners,LimitedPartnershipsNumLimPartners,
  def limitedPartnershipFromMap = {
    LimitedPartnerships(
      num_gen_partners = map("LimitedPartnershipsNumGenPartners").toInt,
      num_lim_partners = map("LimitedPartnershipsNumLimPartners").toInt
    )
  }

  // PreviousName_1CONDATE,PreviousName_1CompanyName,
  // PreviousName_2CONDATE,PreviousName_2CompanyName,
  // PreviousName_3CONDATE,PreviousName_3CompanyName,
  // PreviousName_4CONDATE,PreviousName_4CompanyName,
  // PreviousName_5CONDATE,PreviousName_5CompanyName,
  // PreviousName_6CONDATE,PreviousName_6CompanyName,
  // PreviousName_7CONDATE,PreviousName_7CompanyName,
  // PreviousName_8CONDATE,PreviousName_8CompanyName,
  // PreviousName_9CONDATE,PreviousName_9CompanyName,
  // PreviousName_10CONDATE,PreviousName_10CompanyName

  def previousNamesFromMap = {
    def getPrevName(i: Int) = {
      PreviousName(
        condate = map(s"PreviousName_${i}CONDATE"),
        company_name = map(s"PreviousName_${i}CompanyName")
      ).?
    }
    PreviousNames(
      previous_name_1 = getPrevName(1),
      previous_name_2 = getPrevName(2),
      previous_name_3 = getPrevName(3),
      previous_name_4 = getPrevName(4),
      previous_name_5 = getPrevName(5),
      previous_name_6 = getPrevName(6),
      previous_name_7 = getPrevName(7),
      previous_name_8 = getPrevName(8),
      previous_name_9 = getPrevName(9),
      previous_name_10 = getPrevName(10)
    )
  }

}
