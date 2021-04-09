package jacekAssignment

import org.apache.spark.sql.SparkSession

object JsonSources {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("JsoFlatten").master("local[2]").getOrCreate()
    val df = spark.read.format("json").option("inferSchema", "true").option("multiline", "true").
      load("/Users/sharadsingh/Documents/spark3maven/src/main/resources/jsonfiles/*.json")

    df.show()
  }
}
