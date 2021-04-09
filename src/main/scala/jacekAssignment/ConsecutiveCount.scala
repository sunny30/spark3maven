package jacekAssignment

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object ConsecutiveCount {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("commulativeFrequency").master("local[2]").getOrCreate()
    import spark.implicits._

    val df = spark.read.option("delimeter",",").option("header","true").csv("/Users/sharadsingh/Documents/spark3maven/src/main/scala/jacekAssignment/cf2.csv")
    df.printSchema()
    df.show()
    val windowSpec = Window.partitionBy(col("id")).orderBy(col("time").asc)

    val in1 = df.withColumn("rank",rank().over(windowSpec))
    val in2 = in1.withColumn("group",col("time")-col("rank"))
    val groupByGroup = in2.groupBy(col("id"),col("group"))
    val out = groupByGroup.agg(count("*").as("count"))

    out.groupBy(col("id")).agg(max(col("count")).as("max_cn")).show()
  }
}
