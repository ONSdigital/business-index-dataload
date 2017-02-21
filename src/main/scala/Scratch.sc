import java.io.Serializable

import scala.util.{Failure, Success, Try}
import org.joda.time.DateTime
import com.github.nscala_time.time.Imports._
import uk.gov.ons.bi.dataload.model.PayeRec

def bodgeToMMMYY(strOpt: Option[String]) = {
  // Some PAYE "month" strings are 4 char, not 3 e.g. "Sept98"
  // This function bodges them to 3 chars for consistency e.g. "Sep98".
  strOpt.map { s =>
    val (mon, yr) = s.partition(!_.isDigit)
    val mmm = mon.substring(0,3)
    (mmm + yr)
  }

}

def getLastUpdOpt(str: Option[String]): Option[DateTime] = {
  // Some PAYE month strings are 4 chars, not 3 e.g. "Sept98".
  // This bit bodges them to 3 chars for consistency e.g. "Sep98".
  val mmmYy = str.map { s =>
    val (mon, yr) = s.partition(!_.isDigit)
    val mmm = mon.substring(0,3)
    (mmm + yr)
  }
  // Now convert the string to a MMMyy date (if possible)
  val fmt = DateTimeFormat.forPattern("MMMyy")
  // unpack the Option
  mmmYy.getOrElse("") match {
    case "" => None
    case s: String => Try {
      DateTime.parse(s, fmt)
    } match {
      case Success(d: DateTime) => Some(d)
      case _ => None
    }
  }
}

val upd1: Option[String] = Some("Jun97")
val upd2: Option[String] = Some("Dec14")
val upd3: Option[String] = Some("BadStr")
val upd4: Option[String] = None
val upd5: Option[String] = Some("Sept16")

case class Fubar(decJobs: Option[Double], marJobs: Option[Double], junJobs: Option[Double],
                 sepJobs: Option[Double], jobsLastUpd: Option[String])

val fubar1 = Fubar(Some(12D), Some(3D), Some(6D), Some(9D), upd5)
val fubar2 = Fubar(Some(12D), Some(3D), Some(6D), Some(9D), upd3)
val fubar3 = Fubar(Some(12D), Some(3D), Some(6D), Some(9D), upd1)

val fubars = Seq(fubar1, fubar2, fubar3)

def getLatestJobsForPayeRec(fubar: Fubar) = {

  val upd: Option[DateTime] = getLastUpdOpt(fubar.jobsLastUpd)

  val jobs: Option[Double] = upd.flatMap { d =>

    d.getMonthOfYear match {
      case 3 => fubar.marJobs
      case 6 => fubar.junJobs
      case 9 => fubar.sepJobs
      case 12 => fubar.decJobs
      case _ => None
    }
  }
  (upd, jobs)
}


val jobUpdates: Seq[(Option[DateTime], Option[Double])] = fubars.map(getLatestJobsForPayeRec)

// Allow for empty sequence
jobUpdates.sorted.reverse match {
  case Nil => None
  case xs => xs.head._2
}


