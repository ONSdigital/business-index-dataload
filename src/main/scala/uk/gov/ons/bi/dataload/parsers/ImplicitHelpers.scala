package uk.gov.ons.bi.dataload.parsers

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
  * Created by Volodymyr.Glushak on 08/02/2017.
  */
object ImplicitHelpers {

  implicit class AsOption[T](x: T) {
    def ? = Option(x)
  }

  implicit class TypedString(s: String) {
    def asDateTime = parseDate(s)

    def asIntOpt = Option(s).map(_.toInt)

    def asDateTimeOpt = Option(s).map(x => parseDate(x))
  }

  def parseDate(s: String) = {
    val dtfm = DateTimeFormat.forPattern("dd/MM/yy")
    dtfm.parseDateTime(s)
  }

}
