package jacekAssignment

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object Pivot {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("pivot").master("local[2]").getOrCreate()
    import spark.implicits._
    val input = Seq(
      Seq("a","b","c"),
      Seq("X","Y","Z"),
      Seq("A","B","C","D")).toDF("array")

    input.select(col("array"),posexplode(col("array"))).groupBy(col("array")).pivot(col("pos")).agg(first("col")).show()
  }

}
