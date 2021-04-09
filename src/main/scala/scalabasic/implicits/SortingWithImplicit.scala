package scalabasic.implicits

object SortingWithImplicit {

  case class Human(name:String,age:Int){

    implicit def apply(name:String,age:Int):Human = Human(name,age)

   // implicit def cj(human: Human): String = ""

    def cToString():String={
      name+" "+age.toString
    }


  }


  def main(args: Array[String]): Unit = {
    val listOfHuman = List(
      Human("Sharad",33),
      Human("Arpan",22),
      Human("Raj",41)
    )


    implicit def cOrdering:Ordering[Human]=Ordering.fromLessThan(_.age<_.age)

    listOfHuman.sorted.map(x=>println(x.cToString()))

    implicit def stringtoInt(value:String):Int={
       Integer.valueOf(value)
    }
    val num:Int = "3"
    println(num)






  }

}
