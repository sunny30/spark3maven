package scalabasic.implicits

object Implicitsfirst {

  case class Person(name:String){
    def greet:String={
      s"""Hi my name is ${name}"""
    }
  }
  implicit def apply(str:String):Person={
    Person(str)
  }
  def main(args: Array[String]): Unit = {
    println("Sharad".greet)
    implicit val defaultVal = 10
    print(increment(2))
  }


  def increment(x:Int)(implicit amount:Int): Int ={
    x+amount
  }

}
