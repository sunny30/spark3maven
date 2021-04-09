package Streaming

import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StructType}

object StreamingAggregation {

  def firstStreamAggregation(spark:SparkSession):Unit={
    val lines = spark.readStream.format("socket").option("host","localhost").option("port","12345").load()
    val agg = lines.select(count(col("value")).as("line_count"))
    agg.writeStream.format("console").outputMode("complete").trigger(Trigger.ProcessingTime(2000)).start().awaitTermination()

  }

  def numericAggregation(spark:SparkSession):Unit = {
    val lines = spark.readStream.format("socket").option("host","localhost").option("port","12345").load()
    val df  = lines.select(col("value").cast(IntegerType).as("number"))
    val df1 = df.select(sum(col("number")).as("sum_number"))
    df1.writeStream.format("console").
      outputMode("complete").
      trigger(Trigger.ProcessingTime(2000)).
      start().awaitTermination()
  }

  def groupByFrequencyDistribution(spark: SparkSession):Unit={

    val lines = spark.readStream.format("socket").option("host","localhost").option("port","12345").load()
    val df = lines.select(col("value").as("token"))
    val groupBy = df.groupBy(col("token"))
    val out = groupBy.agg(count(col("token")).as("frequency"))
    out.writeStream.format("console").
      outputMode("complete").
      trigger(Trigger.ProcessingTime(2000)).
      start().awaitTermination()

  }

  def getJsonSchema(json:String,spark:SparkSession):StructType={
    import spark.implicits._
    val jsonStr = """{"id":"1","name":"Sharad"}"""
    spark.read.json(Seq(jsonStr).toDS).schema
  }

  def readJsonStream(spark:SparkSession):Unit={
    //val ssc = new StreamingContext(spark.conf,)
    val lines = spark.readStream.format("socket").option("host","localhost").option("port","12345").load()
    //lines.show()

  //  val q2 = lines.select(col("value")).writeStream.format("console").outputMode("append").start()
    val schema = getJsonSchema("",spark)
    val df = lines.
      select(from_json(col("value"),schema).as("value1")).select("value1.id","value1.name")
    val quer2 = df.writeStream.format("console").
      outputMode("append").
      trigger(Trigger.ProcessingTime(2000)).
      start()
    //query1.awaitTermination()
    quer2.awaitTermination()
   // q2.awaitTermination()
  //  df.show()

    //val schema = getJsonSchema(lines.as[String].flatMap(_.split("")),spark)
  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("streams-aggregation").master("local[2]").getOrCreate()
   // firstStreamAggregation(spark)
   // numericAggregation(spark)
   // groupByFrequencyDistribution(spark)
    readJsonStream(spark)


  }

}
