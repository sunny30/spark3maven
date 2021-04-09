package jacekAssignment

import org.apache.spark.sql.types.{ArrayType, NullType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object JsonFlatten {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("JsoFlatten").master("local[2]").getOrCreate()

    val df = spark.read.format("json").option("inferSchema", "true").option("multiline", "true").
      load("/Users/sharadsingh/Documents/spark3maven/src/main/scala/jacekAssignment/cf6.json")

    df.schema.fields.map(x => println(x.name))

    df.select(col("bike.name").as("bike_name"), col("age"),col("name"), explode(col("bike.models")).as("bike_model")).show()
    val flatDF = flattenDF(df)

    val fdf = df.flattenDF(df)

    flatDF.show()
  }


    implicit def flattenDF(df:DataFrame):DataFrame={
      val fields = df.schema.fields
      val len = df.schema.length
      val fieldNames = df.schema.fields.map(x=>x.name)

      for(i<-0 to len-1){
        val cfield = fields(i)
        cfield.dataType match {

          case arrayType: ArrayType =>{
            val columns = fieldNames.filter(_!=cfield.name).map(x=>expr(x))++Seq(explode(col(cfield.name)).as(cfield.name))
            val flatdf = df.select(columns:_*)
            return flattenDF(flatdf)

          }
          case structType: StructType=>{
            val columns = fieldNames.filter(_!=cfield.name).map(x=>expr(x))++structType.fields.map(x=> col(cfield.name+"."+x.name).as(cfield.name+"_"+x.name))
            val flatDF = df.select(columns:_*)
            return flattenDF(flatDF)

          }
          case _ =>

        }

      }
      df
    }

  implicit class DF(df:DataFrame){

    implicit def flattenDF(df:DataFrame):DataFrame={
      val fields = df.schema.fields
      val len = df.schema.length
      val fieldNames = df.schema.fields.map(x=>x.name)

      for(i<-0 to len-1){
        val cfield = fields(i)
        cfield.dataType match {

          case arrayType: ArrayType =>{
            val columns = fieldNames.filter(_!=cfield.name).map(x=>expr(x))++Seq(explode(col(cfield.name)).as(cfield.name))
            val flatdf = df.select(columns:_*)
            return flattenDF(flatdf)

          }
          case structType: StructType=>{
            val columns = fieldNames.filter(_!=cfield.name).map(x=>expr(x))++structType.fields.map(x=> col(cfield.name+"."+x.name).as(cfield.name+"_"+x.name))
            val flatDF = df.select(columns:_*)
            return flattenDF(flatDF)

          }
          case _ =>

        }

      }
      df
    }


  }


  }

