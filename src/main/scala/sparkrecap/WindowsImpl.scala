package sparkrecap

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object WindowsImpl {

  case class Salary(depName: String, empNo: Long, salary: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("sparkbasic").master("local[2]").getOrCreate()

    import spark.implicits._
    val empsalary = Seq(
      Salary("sales", 1, 5000),
      Salary("personnel", 2, 3900),
      Salary("sales", 3, 4800),
      Salary("sales", 4, 4800),
      Salary("personnel", 5, 3500),
      Salary("develop", 7, 4200),
      Salary("develop", 8, 6000),
      Salary("develop", 9, 4500),
      Salary("develop", 10, 5200),
      Salary("develop", 11, 5200)).toDF()


    val windowSpec  = Window.partitionBy(col("depName"))
    val maxminDF = empsalary.withColumn("maxSalary",max(col("salary")).over(windowSpec)).withColumn("minSalary",min(col("salary")).over(windowSpec))
    maxminDF.show()


  }

}
