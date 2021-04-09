package jacekAssignment

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
object AggregateImpl {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("commulativeFrequency").master("local[2]").getOrCreate()

    val df = spark.read.option("delimeter",",").option("header","true").option("inferSchema","true").
      csv("/Users/sharadsingh/Documents/spark3maven/src/main/scala/jacekAssignment/cf3.csv")
    df.printSchema()

    val out = df.select(max(col("population").cast(LongType)).as("max_population"),avg(col("population").cast(LongType)).as("avg_population"))


    out.show()

    val join = df.join(out,df.col("population")===out.col("max_population"),"leftsemi")
    df.rdd.setName("Sharad")
    join.show()
  }

}
