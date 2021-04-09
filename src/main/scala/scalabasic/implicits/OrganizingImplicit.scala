package scalabasic.implicits

object OrganizingImplicit {

  case class Person(name:String,age:Int)
  def main(args: Array[String]): Unit = {
    //this will take precedence with Scala.predef
    implicit val reverseOrder:Ordering[Int]  = Ordering.fromLessThan(_>_)
    val a = List(1,4,3,2,5).sorted
    println(a)
    //sorted takes predef for implicits for criterion and it is present in Scala.predef


    /*
      Implicits
        vals/var
        object
        -accessor method = defs with no parentheses (def with no arg should have any parenthese)
     */

    implicit val PersonSortOrder:Ordering[Person]=Ordering.fromLessThan(_.age<_.age)
    println(List(Person("Sharad",33),Person("Rajat",31)).sorted)

    /*
      Implicit scope
        normal scope--local scope
        imported scope
        companion objects of all type
     */
    /*
      when defining an implicit val
     */




  }
}
