package Streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
object KafkaStream {

  val spark = SparkSession.builder().appName("kadka").
    master("local[2]").getOrCreate()

  def readFromKafka():Unit={
    val kafkaDF = spark.readStream.format("kafka").
      option("kafka.bootstrap.servers","localhost:9092").
      option("subscribe","rockthejvm").load()

    kafkaDF.select(col("topic"),col("value").cast(StringType).as("valueInString")).writeStream.format("console").
      outputMode("append").start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readFromKafka()
  }
}
