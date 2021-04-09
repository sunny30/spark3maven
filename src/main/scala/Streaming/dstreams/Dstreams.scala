package Streaming.dstreams

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
object Dstreams {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Dstreams").master("local[2]").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext,Seconds(1))

  }
}
