package jacekAssignment

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GeneratingExamResult {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("commulativeFrequency").master("local[2]").getOrCreate()

    val df = spark.read.option("delimeter",",").option("header","true").csv("/Users/sharadsingh/Documents/spark3maven/src/main/scala/jacekAssignment/cf5.csv")
    df.printSchema()

    df.show()

    val out = df.groupBy(col("ParticipantID"),col("GeoTag"),col("Assessment")).pivot("Qid").
      agg(first("AnswerText").alias("Qid"))
    out.show()



  }
}
