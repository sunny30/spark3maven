package Streaming

import Streaming.KafkaStream.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object PostgresStreaming {

  case class Entity(
                   id:Int,
                   name:String

                   )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().
      appName("posgres").
      master("local[2]").getOrCreate()

    import spark.implicits._
    val df = Seq(Entity(1,"Sharad")).toDF() ;

    val jdbcUrl = "jdbc:postgresql://localhost:5432/rtjvm"
    val driver  = "org.postgresql.Driver"
    val user = "docker"
    val password = "docker"


    val entityDF = spark.readStream.format("kafka").
      option("kafka.bootstrap.servers","localhost:9092").
      option("subscribe","rockthejvm").load()
    //entityDF.select(col("value").cast("string")) .alias("csv").writeStream.format("console").start().awaitTermination()

    val cdf =entityDF.select(col("value").cast("string")).
      alias("csv").select(col("csv.value"))

      cdf.select(element_at(split(col("csv.value"),","),1).as("id").cast(StringType),
        element_at(split(col("csv.value"),","),2).as("name").cast(StringType)).
      writeStream.format("console").
      outputMode("append").start().awaitTermination()

//    val interval = entityDF.select(col("value").cast("string")) .alias("csv")
//    val fdf = interval.select(array_position(split(col("csv.value"),","),1).as("id").cast(IntegerType),array_position(split(col("value"),","),1).as("name").cast(StringType))
//    fdf.writeStream.format("csv").
//      outputMode("append").
//      start().
//      awaitTermination()


  }

}
