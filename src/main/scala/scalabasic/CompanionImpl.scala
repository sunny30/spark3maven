package scalabasic

object CompanionImpl{
  def apply(name: String, age: Int): CompanionImpl = {
    val p = new CompanionImpl(name, age)
    p.map = Map.empty[String,String]
    p
  }
}

 class CompanionImpl(val name:String, val age:Int) {

  var map = Map.empty[String,String]

   def populateMap():Unit={
     map += (name->age.toString)

   }

   def print:Unit={
      println(s"""${name} has age ${age} and map has size ${map.size}""")
   }



}


object ms{
  def main(args: Array[String]): Unit = {
    val person = CompanionImpl("Sharad",32) ;
    person.populateMap()
    person.print
  }
}
