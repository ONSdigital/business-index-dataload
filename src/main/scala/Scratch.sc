

val mydata: Seq[String] = Array("CH001","CH002")

def getCompanyNo(arr: Seq[String]) = {
  arr.headOption match {
    case Some(ch) => ch
    case None => ""
  }
}


getCompanyNo(mydata)
