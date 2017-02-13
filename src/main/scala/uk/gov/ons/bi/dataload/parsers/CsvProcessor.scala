package uk.gov.ons.bi.dataload.parsers

/**
  * Created by Volodymyr.Glushak on 08/02/2017.
  */
object CsvProcessor {

  val Delimiter = ","
  val Eol = System.lineSeparator


  /**
    * Create List of Map[String, String] from provided CSV
    * Keys in Map are headers from first line
    *
    * @param csvString - csv string
    * @return - Map of data
    */
  def csvToMap(csvString: Seq[String]) = {
    val csv = csvString.map { rec =>
      rec.split(Delimiter)
    }.toList match {
      case header :: data => data.map { d => header zip d toMap }
      case header :: Nil =>
        sys.error("CSV file is empty")
    }
    csv
  }
}


