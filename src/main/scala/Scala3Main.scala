import org.apache.spark.sql.{DataFrame, SparkSession}
import io.delta.tables._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.functions._
object Scala3Main {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder().master("local[*]")
      .appName("databricks-sample1")
      .config("spark.default.parallelism", 4)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .getOrCreate()
    spark.conf.set("spark.sql.debug.maxToStringFields", 10000)
    val data = spark.range(5, 10).toDF()
    data.write.format("delta").mode("overwrite").save("/tmp/delta-table")
    data.show()
    val df = spark.read.format("delta").load("/tmp/delta-table")
    df.show()
    val updatedDf = updateDelta("/tmp/delta-table")
    updatedDf.show()
    //val updateExpressionString = """when(col("id")%2===0,col("id")+100).otherwise(col("id"))"""
    val updateExpressionString = "CASE WHEN ((`id` % CAST(2 AS BIGINT)) = CAST(0 AS BIGINT)) THEN (`id` + CAST(100 AS BIGINT)) ELSE `id` END"
    updateWithoutDelta("id","",data,updateExpressionString).show()
  }

  def updateDelta(path:String):DataFrame={
    val deltaTable = DeltaTable.forPath(path)
    deltaTable.update(
      condition=expr("id%2==0"),
      set = Map("id" -> expr("id + 100"))
    )
    val df = deltaTable.toDF
    df
  }

  def updateWithoutDelta(colName:String, conditionExpr:String,input:DataFrame,updateExpression:String):DataFrame={
    val tempDF = input.withColumn("new_"+colName,expr((updateExpression)))
    //val temp2DF = tempDF.filter(expr(conditionExpr))
//    val joinDF = input.as("main").join(tempDF.as("delta"),expr("main.id==delta.new_id"),"rightouter")
//    val updateDF = joinDF.withColumn(colName,when(col("new_"+colName).isNull,col("main.id").otherwise(col("delta.id"))))
    val updateDF = tempDF.drop(colName).withColumnRenamed("new_"+colName,colName)
    updateDF
  }



}
