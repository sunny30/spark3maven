package swa

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object tekion {
  case class batch1(id:String,value:String)
  case class batch2(id:String,value:String,extraValue:String)
  var names = Seq.empty[String]
  def main(args: Array[String]): Unit = {
     val sparkSession = SparkSession.builder().appName("tekion").master("local").getOrCreate()

    val df = sparkSession.read.option("inferSchema","true").option("multiline","true").json("/Users/sharadsingh/Documents/spark3maven/src/main/scala/swa/1.json")
    val map = Map.empty[String,String]
   // schemaRecursion(df.schema,map)
    //var names = Seq.empty[String]
    df.schema.fields.map(f=>fieldRecursion(f,""))
    names.size
    import  sparkSession.implicits._
    val df1 = Seq(batch1("1","1")).toDF


//source df 100
// 10 partition

    /*
        source df partition
          36 partition

          having december
          //cdc serialize

          tenant dealer migration_date migration_epoch_time sourceId
     */

  }

  def fieldRecursion(structfield: StructField,name:String): Unit ={
    var completeName = ""
    var list : Seq[String] = Seq.empty[String]
    if(structfield.dataType.typeName.equalsIgnoreCase("struct"))
      structfield.dataType.asInstanceOf[StructType].fields.map(f=>fieldRecursion(f,name+structfield.name+"."))
    else{
      completeName = name+structfield.name
      names = this.names:+completeName
      println(completeName)
    }
  }
//
//
//  def schemaRecursion(schema:StructType, map:Map[String,String]):Map[String,String]={
//
//          schema.fields.map(field=>{
//            if(field.isInstanceOf[StructType]) {
//              schemaRecursion(field.asInstanceOf[StructType],map)
//            } else
//              (field.name->field.dataType.toString)
//
//
//
//          }).toMap[String,String]
//
//
//  }
}
