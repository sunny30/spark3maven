package scalabasic.implicits

object TypeClasses {

  trait HTMLWritable{
    def toHTML:String

  }

  case class User(name:String,age:Int,email:String) extends HTMLWritable{
    override def toHTML:String = s"""<div>$name $age <a href=$email/></div>"""
  }

  def main(args: Array[String]): Unit = {
    //dis advantage you can write only for your own case class
    println(User("sh",32,"sharadsingh8@gmail.com").toHTML)

    implicit object IntSerializer extends HTMLSerializer[Int]{
      override def serialize(value: Int): String = value.toString
    }
    println(HTMLSerializer.serialize(43))
   // println(Equals.equals("hello","HeLLo"))

    val sharad = User("Sharad",34,"sharadsingh8@gmail.com")
    val sunny = User("sharad",34,"SHARADSINGH8@gmail.com")


   import TypeClasses.UserEqualizer._

    println(Equals(sharad,sunny))
  }

/*companion pattern below*/
  trait HTMLSerializer[T]{
    def serialize(value:T):String
  }

  object HTMLSerializer{
    def serialize[T](value:T)(implicit serializer: HTMLSerializer[T]):String = serializer.serialize(value)
  }

  trait Equals[T]{
    def equals(a:T,b:T):Boolean

    def apply(a:T,b:T): Boolean
  }

  object Equals {
    def equals[T](a:T,b:T)(implicit equal:Equals[T]):Boolean=equal.equals(a,b)

    def apply[T](a:T,b:T)(implicit equal:Equals[T]): Boolean = equal.equals(a,b)

  }

  object StringEqualizer extends Equals[String]{
    override def equals(a: String, b: String): Boolean = a.compareToIgnoreCase(b)==0

    override def apply(a:String,b:String): Boolean=equals(a,b)
  }

  implicit object UserEqualizer extends Equals[User]{
    override def equals(a: User, b: User): Boolean = a.age==b.age && a.name.equalsIgnoreCase(b.name) && a.email.equalsIgnoreCase(b.email)

    override def apply(a: User, b: User): Boolean = UserEqualizer.equals(a,b)
  }




}
