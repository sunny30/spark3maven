import java.sql.Date

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ScalaSCD2Merge {

  case class Customer(customerId: Int,
                      address: String,
                      current: Boolean,
                      effectiveDate: String,
                      endDate: String)
  case class CustomerUpdate(customerId: Int,
                            address: String,
                            effectiveDate: String)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("databricks-sample1")
      .config("spark.default.parallelism", 4)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .getOrCreate()

    import spark.implicits._

    val mainDF = Seq(
      Customer(1, "current address for 1", true, "2018-02-01", null),
      Customer(2, "current address for 2", true, "2018-02-01", null),
      Customer(3, "current address for 3", true, "2018-02-01", null)
    ).toDF()

    val cdcDF = Seq(
      Customer(1, "new current address for 1-1", false, "2018-02-01", "2018-02-03"),
      Customer(2, "new current address for 2-1", true, "2018-02-05", null),
      Customer(3, "new current address for 3-1", true, "2018-02-07", null),
      Customer(4, "current address for 4-1", true, "2018-02-11", null)
    ).toDF()

//    val naturalKeys = Seq("customerId")
//    val whenMatchUpdateExpression: Map[String, String] = Map(
//     // ("main.current" -> "lit(false)"),
//      ("endDate" -> "cdc.effectiveDate")
//    //  ("address" -> "cdc.address"))
//    )
//    scd2Merge(spark,mainDF, cdcDF, naturalKeys, null, whenMatchUpdateExpression)

    val conditionString: String = "customerId=1"
    val deleteDF = deleteData(mainDF,conditionString)
    deleteDF.show()
  }

  def scd2Merge(spark: SparkSession,
                mainDF: DataFrame,
                cdcDF: DataFrame,
                naturalKeys: Seq[String],
                cdcColumns: Seq[String],
                update: Map[String, String]): DataFrame = {

    val mainColumns = mainDF.columns
    val cdcColumns = cdcDF.columns
    val mainDFWithMerge =
      mainDF.withColumn("mergeKey", concat(naturalKeys.map(mainDF.col): _*))
    val cdcDFWithMerge =
      cdcDF.withColumn("mergeKey", concat(naturalKeys.map(cdcDF.col): _*))

    val fullOuterJoin = mainDFWithMerge
      .alias("main")
      .join(cdcDFWithMerge.alias("cdc"),
            expr("main.mergeKey=cdc.mergeKey"),
            "fullOuter")
    val insertNewDF = insertNewRecords(fullOuterJoin, mainColumns).drop("mergeKey")
    val insertUpdateDF = insertUpdateRecords(fullOuterJoin, mainColumns).drop("mergeKey")
    val matchUnionRecords = fullOuterJoin.filter(col("main.mergeKey").isNotNull && col("cdc.mergeKey").isNotNull)
    val updateMainDF = updateMainRecords(matchUnionRecords,update.toArray)
    val updateMainFinalDF = dropAndOrderUpdateMainDataSet(cdcColumns,updateMainDF,mainColumns)
    val scd2OutDF = insertNewDF.union(insertUpdateDF).union(updateMainFinalDF)
    scd2OutDF.show()
    scd2OutDF
  }

  def dropAndOrderUpdateMainDataSet(cdcColumns:Seq[String],in:DataFrame,mainColumns:Seq[String]):DataFrame={
      val cdcQualifiedColumns = cdcColumns.map(x=>col("cdc."+x)).seq
      val out = dropRecursive(in,cdcQualifiedColumns,0)
      out.select(mainColumns.map(col):_*)
  }

  def dropRecursive(in:DataFrame,cdcColumns:Seq[Column],index:Int): DataFrame ={
    if(index>cdcColumns.size-1)
      in
    else{
      val out = in.drop(cdcColumns(index))
      dropRecursive(out,cdcColumns,index+1)
    }
  }

  def insertUpdateRecords(fullOuterJoin: DataFrame,
                          mainDFColumns: Seq[String]): DataFrame = {
    fullOuterJoin
      .filter(col("main.mergeKey").isNotNull && col("cdc.mergeKey").isNotNull)
      .select("cdc.*")
  }

  def insertNewRecords(fullOuterJoin: DataFrame,
                       mainDFColumns: Seq[String]): DataFrame = {
    fullOuterJoin
      .filter(col("main.mergeKey").isNull && col("cdc.mergeKey").isNotNull)
      .select("cdc.*")
  }

  def updateMainRecords(
      matchedOuterJoinDF: DataFrame,
      updateCollection: Array[(String, String)]): DataFrame = {

    updateMainRecordsRecursive(matchedOuterJoinDF, updateCollection, 0)

  }

  private def updateMainRecordsRecursive(
      matchedOuterJoinDF: DataFrame,
      updateCollection: Array[(String, String)],
      index: Int): DataFrame = {
    if (index > updateCollection.size - 1) {
      matchedOuterJoinDF
    } else {
      val temDF = matchedOuterJoinDF.transform(
        updateColumnValue(updateCollection(index)._1,
                          updateCollection(index)._2))
      updateMainRecordsRecursive(temDF, updateCollection, index + 1)
    }
  }
  def updateColumnValue(columnName: String, expressionString: String)(
      matchedOuterJoinDF: DataFrame): DataFrame = {
    val nc = columnName.replace('.','_')+"_new"
    //val newExpressionString = "main."+expressionString
    val outJoinTemp =
      matchedOuterJoinDF.withColumn( nc, expr(expressionString))
    val updateDF = outJoinTemp
      .drop(col("main."+columnName)).drop(col("cdc."+columnName))
        .withColumnRenamed(nc,columnName)
    updateDF
  }

  def generateMergeColumnsExpression(rowIdColumnName: String,
                                     columnCollection: Seq[String],
                                     prefix: String): List[Column] = {
    columnCollection
      .map(columnName =>
        expr(
          s"""if($prefix.$rowIdColumnName IS NULL, main.$columnName,$prefix.$columnName) AS $columnName"""
      ))
      .toList
  }

  def deleteData(df:DataFrame,condition:String):DataFrame={
    val filterDf = df.filter(expr(condition))
    val leftDF = df.as("left")
    val rightDF = filterDf.as("right"
    )
    val out = leftDF.join(rightDF,col("left.customerId")===col("right.customerId"),"left_anti")
    out
  }
  //insert into table target(
  // select t3.a/t2.b as cc,b,c
  //  from table t,t2,t3
  // where a>50
  //)
}
