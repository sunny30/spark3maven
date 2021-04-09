import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object DisneyPOC {

  case class DD(id:String,value:Int)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("databricks-sample1")
      .config("spark.default.parallelism", 4)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .getOrCreate()
    spark.conf.set("spark.sql.debug.maxToStringFields", 10000)
    import spark.implicits._
    val leftDF = Seq(DD("1",1),DD("2",3),DD("3",5)).toDF()
    val rightDF = Seq(DD("1",1),DD("2",4),DD("3",7)).toDF()

    val joinDF = leftDF.as("left").join(rightDF.as("right"),expr("left.id==right.id"),"leftOuter")
    joinDF.show()
    joinDF.filter(col("right.value")>4).show()

  }

}
