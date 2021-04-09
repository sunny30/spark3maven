package jacekAssignment

import org.apache.spark.sql.catalyst.plans.{FullOuter, JoinType}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

object IfWithJoin {

  case class Record(id:Int,name:String,value:String)

  case class cdcRecord(id:Int,name:String,value:String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("commulativeFrequency").master("local[2]").getOrCreate()

    import spark.implicits._
    val mainRecords = Seq(
      Record(1,"sharad","first"),
      Record(2,"sunny","second"),
      Record(3,"Rajat","third")
    )

    val cdcRecords = Seq(
      cdcRecord(1,"sharad","Third"),
      cdcRecord(2,"sunny","fourth"),
    )

    val mainDf = mainRecords.toDF()
    val cdcDf = cdcRecords.toDF()

    val columnNames = cdcDf.schema.fields.map(x=>x.name)
    val fullOuterDf = mainDf.as("main").join(cdcDf.as("cdc"),mainDf.col("id")===cdcDf.col("id"),"fullouter")
    fullOuterDf.select(gatherColumns(columnNames,"cdc","main","id"):_*).show()

  }


  def gatherColumns(columnNames:Seq[String],prefix:String,otherPrefix:String,naturalKey:String):Seq[Column]={

    columnNames.map(name=>(when(col(s"""$prefix.$naturalKey""").isNull,col(s"""$otherPrefix.$name""")).
      otherwise(col(s"""$prefix.$name"""))).as(name)).seq
  }


}
