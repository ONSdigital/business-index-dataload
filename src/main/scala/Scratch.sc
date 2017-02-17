import scala.util.{Failure, Success, Try}

val bad = "x"

val good =  "123"

def f(x: String):Int = {

  Try {x.toInt * 2} match {
    case Success(n) => n
    case Failure(e) => 0
  }
  }

f(good)
f(bad)
