package jacekAssignment

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CollectListIMpl {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("commulativeFrequency").master("local[2]").getOrCreate()
    val nums = spark.range(5).withColumn("group", col("id") % 2)

    val groupBy = nums.groupBy(col("group"))

    val out = groupBy.agg(collect_list(col("id")))
    out.show()


  }

}
