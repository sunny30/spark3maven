import DisneyPOC.DD
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{DataFrame, SparkSession}

object ScalaTransformAPI {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("databricks-sample1")
      .config("spark.default.parallelism", 4)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .getOrCreate()
    spark.conf.set("spark.sql.debug.maxToStringFields", 10000)
    import spark.implicits._
    val leftDF = Seq(DD("1",1),DD("2",3),DD("3",5)).toDF()
    val rightDF = Seq(DD("1",1),DD("2",4),DD("3",7)).toDF()

    val joinDF = leftDF.transform(transforExample(rightDF))
  }


  def transforExample(in:DataFrame)(df:DataFrame): DataFrame ={
    val joinDF = df.as("left").join(in.as("right"),expr("left.id==right.id"),"leftOuter")
    joinDF

  }
}
