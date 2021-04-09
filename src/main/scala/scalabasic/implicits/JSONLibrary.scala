package scalabasic.implicits

import java.util.Date

object JSONLibrary {

  case class User(name:String,age:Int)
  case class Post(content:String,createdAt:Date)
  case class Feed(user: User,post: List[Post])


  trait JSONValue{
    def stringfy:String
  }

  case class JSONString(value:String) extends JSONValue{
    override def stringfy: String = "\""+ value+ "\""
  }

  case class JSONNumber(value:Int) extends JSONValue{
    override def stringfy: String = value.toString
  }

  case class JSONDate(value:Date) extends JSONValue{
    override def stringfy: String = value.toString
  }

  case class JSONList(values:List[JSONValue]) extends JSONValue{
    override def stringfy: String = {
      values.map(x=>x.stringfy)
    }.mkString("[",",","]")
  }

  case class JSONObject(values:Map[String,JSONValue]) extends JSONValue{
    override def stringfy: String = {
      values.map(x=>{
        val key = x._1 ;
        val value = x._2
        "\""+key+"\":"+"\""+value.stringfy+"\""
      }).mkString("{",",","}")
    }
  }

  trait JSONConvrter[T]{
    def convert(value:T):JSONValue
  }

  implicit class JSONops[T](value:T){
    def toJSON(implicit converter:JSONConvrter[T]):JSONValue=converter.convert(value)
  }

  implicit object StringConverter extends JSONConvrter[String]{
    override def convert(value: String): JSONValue = JSONString(value)
  }

  implicit object NumberConverter extends JSONConvrter[Int]{
    override def convert(value: Int): JSONValue = JSONNumber(value)
  }

  implicit object UserConverter extends JSONConvrter[User]{
    override def convert(value: User): JSONValue = {
      JSONObject(
        Map(
          "name"->JSONString(value.name),
          "age"->JSONNumber(value.age),

        )
      )
    }
  }

  implicit object PostConverter extends JSONConvrter[Post]{
    override def convert(value: Post): JSONValue = {
      JSONObject(
        Map(
          "content"->JSONString(value.content),
          "date"->JSONDate(value.createdAt)
        )
      )
    }
  }

  implicit object PostsConverter extends JSONConvrter[List[Post]]{
    override def convert(value: List[Post]): JSONValue = {
      JSONList(value.map(_.toJSON))
    }
  }

  implicit object FeedConverter extends JSONConvrter[Feed]{
    override def convert(value: Feed): JSONValue = {

      JSONObject(Map(
        "user"->value.user.toJSON,
        "posts"->value.post.toJSON
      )
      )
    }
  }


  def main(args: Array[String]): Unit = {

    val data = JSONObject(Map(
      "user"->JSONString("sunny"),
      "posts"->JSONList(List(
        JSONString("scala rocks"),
        JSONNumber(30)
      ))
    ))

    println(data.stringfy)

    val sunny = User("sunny",33)
    val post1 = Post("hello",new Date(System.currentTimeMillis()))
    val post2 = Post("hey",new Date(System.currentTimeMillis()))
    val feed = Feed(sunny,List(post1,post2))
    println(feed.toJSON.stringfy)
  }
}
