package jacekAssignment

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object RunningCommulativeFrequency {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("commulativeFrequency").master("local[2]").getOrCreate()

    val df = spark.read.option("delimeter",",").option("header","true").csv("/Users/sharadsingh/Documents/spark3maven/src/main/scala/jacekAssignment/cf.csv")
    df.printSchema()
    val windowSpec = Window.partitionBy(col("department")).orderBy(col("time").asc)

    val resultDF = df.withColumn("running_total",sum(col("items_sold")).over(windowSpec))
    resultDF.show()
  }
}
