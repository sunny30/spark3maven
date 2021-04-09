package jacekAssignment

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object ConcatWsImpl {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("commulativeFrequency").master("local[2]").getOrCreate()
    import spark.implicits._
    val words = Seq(Array("hello", "world")).toDF("words")

    words.withColumn("solution",concat_ws(" ",col("words"))).show()

  }
}
