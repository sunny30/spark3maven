import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import io.delta.tables._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._


object Scala3UpdateSWA {

  case class RES_PAX_AIR
  (
    RES_SYS_ID: String,
    PNR_REC_LOC_ID: String,
    PNR_CRE_CENT_TZ: String,
    PNR_ENVLP_NUM: Long,
    EFF_FM_CENT_TZ: String,
    EFF_TO_CENT_TZ: String,
    MAX_EFF_TO_FLAG: Long,
    MIN_EFF_FM_FLAG: Long,
    CUR_TS: String
  )
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder().master("local[*]")
      .appName("databricks-sample1")
      .config("spark.default.parallelism", 4)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .getOrCreate()
    spark.conf.set("spark.sql.debug.maxToStringFields", 10000)

    import spark.implicits._

    val keyColumns         = List("RES_SYS_ID", "PNR_REC_LOC_ID")

    val mainDF = Seq(
      RES_PAX_AIR("1A", "DDVR08", "2020-06-23", 1, "2020-06-23 00:00:00", "2099-12-31 00:00:00", 1, 1, ""),
      RES_PAX_AIR("1B", "DDVR09", "2020-06-23", 1, "2020-06-23 00:00:00", "2099-12-31 00:00:00", 1, 1, ""),
      RES_PAX_AIR("1C", "DDVR10", "2020-06-24", 1, "2020-06-24 00:00:00", "2099-12-31 00:00:00", 1, 1, ""),
      RES_PAX_AIR("1D", "DDVR11", "2020-06-24", 1, "2020-06-24 00:00:00", "2099-12-31 00:00:00", 1, 1, ""),
      RES_PAX_AIR("1A", "DDVR12", "2020-06-24", 1, "2020-06-24 00:00:00", "2099-12-31 00:00:00", 1, 1, "")
    ).toDF()

    val seqOfSWARecords = Seq(
      RES_PAX_AIR("1A", "DDVR08", "2020-06-23", 3, "2020-06-23 00:00:00", "2099-12-31 00:00:00", 1, 1, ""),
      RES_PAX_AIR("1A", "DDVR08", "2020-06-23", 4, "2020-06-25 00:00:00", "2099-12-31 00:00:00", 1, 1, ""),
      RES_PAX_AIR("1C", "DDVR10", "2020-06-24", 5, "2020-06-26 00:00:00", "2099-12-31 00:00:00", 1, 1, ""),
      RES_PAX_AIR("1D", "DDVR11", "2020-06-24", 6, "2020-06-27 00:00:00", "2099-12-31 00:00:00", 1, 1, ""),
      RES_PAX_AIR("1E", "DDVR12", "2020-06-24", 7, "2020-06-28 00:00:00", "2099-12-31 00:00:00", 1, 1, "")
    )
    val minFlagColumn      = "MIN_EFF_FM_FLAG"
    val maxFlagColumn      = "MAX_EFF_TO_FLAG"

    val scdHistoricColumns = List("PNR_ENVLP_NUM")



    val cdc = seqOfSWARecords.toDF()
    val updateColumns: Array[String] = mainDF.columns

    val rowsToUpdate = cdc.alias("left")
      .join(mainDF.alias("right"), keyColumns)                                                              // join on key columns
      .where(
        mainDF.col(s"$maxFlagColumn") === lit(1) &&                                          // Only consider TopOfStack (TOS)
          scdHistoricColumns.map {
            c => mainDF.col(c) =!= cdc.col(c)                                            // unequal historic columns
          }.reduce( (x, y) => x || y))
      .select(updateColumns.map(cdc.col): _*).withColumn("mergeKey",concat(keyColumns.map(cdc.col): _*))


    println("rows to update")
    rowsToUpdate.show()


    val distinctPnr = cdc.select(col("PNR_ENVLP_NUM"),col("EFF_FM_CENT_TZ")).distinct().map(r=>(r.get(0).asInstanceOf[Long],r.get(1).asInstanceOf[String])).collectAsList().asScala.toList
    val sortedPnr = distinctPnr.sortWith(_._1<_._1)
    println(sortedPnr)
    val updatedDF = updateSWACdcRecords(rowsToUpdate,mainDF.columns.toList,sortedPnr,"EFF_TO_CENT_TZ","PNR_ENVLP_NUM")
    updatedDF.show()
  }

  def updateColumnValueToSpecificValue(input:DataFrame,columnNames:List[String],sortedPnr:List[(Long,String)],updateColumnName:String,conditionColumnName:String,index:Int):DataFrame={
    if(index>=sortedPnr.size-1){
      input.select(columnNames.map(col):_*)
    }else{
      val temp_updateDF = input.withColumn("new_"+updateColumnName,when(col(conditionColumnName)=== sortedPnr(index)._1,sortedPnr(index+1)._2 ).otherwise(col(updateColumnName)))
      val updatedDF = temp_updateDF.drop(updateColumnName)
      .withColumnRenamed("new_"+updateColumnName,updateColumnName)
      updateColumnValueToSpecificValue(updatedDF,columnNames,sortedPnr,updateColumnName,conditionColumnName,index+1)
    }
  }

  def updateSWACdcRecords(input:DataFrame,columnNames:List[String],sortedPnr:List[(Long,String)],updateColumnName:String,conditionColumnName:String):DataFrame = {
    updateColumnValueToSpecificValue(input,columnNames,sortedPnr,updateColumnName,conditionColumnName,0)
  }


}
