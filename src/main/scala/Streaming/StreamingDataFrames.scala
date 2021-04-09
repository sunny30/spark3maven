package Streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object StreamingDataFrames {
  val spark = SparkSession.builder().appName("streams").master("local[2]").getOrCreate()

  def readFromSocket():Unit={
    val lines = spark.readStream.format("socket").option("host","localhost").option("port","12345").load()
    val shortdf = lines.filter(length(col("value"))<8)
    println(shortdf.isStreaming)

    //action
    val query = shortdf.writeStream.format("console").
      outputMode("append").
      trigger(Trigger.ProcessingTime(2000)).start()
    query.awaitTermination()
  }
  def main(args: Array[String]): Unit = {
      readFromSocket()
  }
}
