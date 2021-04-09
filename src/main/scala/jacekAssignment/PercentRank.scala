package jacekAssignment

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

object PercentRank {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("commulativeFrequency").master("local[2]").getOrCreate()

    val df = spark.read.option("delimeter",",").option("header","true").
      csv("/Users/sharadsingh/Documents/spark3maven/src/main/scala/jacekAssignment/cf1.csv")
    df.printSchema()

    val in = df.withColumn("cons_id",lit(1))
    val windowSpec = Window.partitionBy(col("cons_id")).orderBy(col("Salary").desc)

    val in1 = in.withColumn("percent_rank",percent_rank().over(windowSpec))
    in1.printSchema()
    val out = in1.withColumn("Percentage",when(col("percent_rank")<(lit(".4").cast(DoubleType)),"HIGH").
      when(col("percent_rank")>lit(".4").cast(DoubleType) and(col("percent_rank")<lit(".7").cast(DoubleType)),"AVERAGE").otherwise("LOW"))

    out.drop(col("cons_id")).drop(col("percent_rank")).show()


  }
}
