import java.io.Serializable

import scala.util.{Failure, Success, Try}
import org.joda.time.DateTime
import com.github.nscala_time.time.Imports._


def getLastUpdOpt(str: Option[String]): Option[DateTime] = {

  val fmt = DateTimeFormat.forPattern("MMMyy")
  // unpack the Option
  str.getOrElse("") match {
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

val upds = Seq(upd1, upd2, upd3, upd4)

upds.map(getLastUpdOpt(_)).filter(_.isDefined).reverse.head


case class Fubar(decJobs: Option[Double], marJobs: Option[Double], junJobs: Option[Double],
                 sepJobs: Option[Double], jobsLastUpd: Option[String])

val fubar1 = Fubar(Some(12D), Some(3D), Some(6D), Some(9D), upd1)

val fubar2 = Fubar(Some(12D), Some(3D), None, Some(9D), upd1)

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

val pair = getLatestJobsForPayeRec(fubar2)
