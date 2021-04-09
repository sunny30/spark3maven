package jacekAssignment

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FirstNonNull {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("commulativeFrequency").master("local[2]").getOrCreate()
    import spark.implicits._
    val data = Seq(
      (None, 0),
      (None, 1),
      (Some(2), 0),
      (None, 1),
      (Some(4), 1)).toDF("id", "group")
    val groupBy = data.filter(col("id").isNotNull).groupBy(col("group"))

    groupBy.agg(first(col("id"))).show()
  }
}
