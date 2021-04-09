package scalabasic.implicits

object TypeClassesEx {

  trait eq[T]{
    def ===(value:T,anothervalue:T):Boolean
  }
  implicit object StringEqualizer extends eq[String]{
    override def ===(value:String,anotherValue:String ): Boolean = value.compareToIgnoreCase(anotherValue)==0
  }

  implicit class tem[T](value:T){
    def ===(anothervalue: T)(implicit coneq:eq[T]): Boolean = coneq.===(value,anothervalue)
  }



  def main(args: Array[String]): Unit = {

    println("john" === "John1")
  }
}
