package jacekAssignment

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object ConvertDataFrameToSingleRowMatrix {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("commulativeFrequency").master("local[2]").getOrCreate()
    import spark.implicits._

    val df = spark.read.option("delimeter",",").option("header","false").csv("/Users/sharadsingh/Documents/spark3maven/src/main/scala/jacekAssignment/cf4.csv")
    df.printSchema()
    df.show()
    df.groupBy().pivot(col("_c0")).agg(first(col("_c1"))).show()

  }
}
