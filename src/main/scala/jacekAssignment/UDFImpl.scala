package jacekAssignment

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object UDFImpl {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("UDF").master("local[2]").getOrCreate() ;
    import  spark.implicits._
    val in = Seq(
      ("Sharad","hello"),
      ("Sunny","hi")
    ).toDF("name","slogan")

    in.show()
    spark.udf.register("myUpper",myupper(_:String):String)
    val myUpper = udf(myupper(_:String):String)

    in.select(col("name"),col("slogan"),myUpper(col("slogan")).as("upper_slogan")).show()

  }

  def myupper(value:String):String={
    value.toUpperCase
  }
}
