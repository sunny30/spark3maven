package jacekAssignment

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
object DateTimeImpl {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("commulativeFrequency").master("local[2]").getOrCreate()
    spark.sqlContext.setConf("dateFormat", "yyyy-MM-dd")
    import spark.implicits._

    val in = Seq(
      ("2019-01-23","name1"),
      ("2019-10-31","name")
    ).toDF("dates","name")

    val out = in.select(col("name"),to_date(col("dates"),"yyyy-mm-dd").as("date"))
    out.show()
    out.printSchema()
  }
}
