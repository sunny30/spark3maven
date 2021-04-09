import ScalaPaxAir.RES_PAX_AIR
import io.delta.tables.DeltaTable
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Scala3Delta {
  case class RES_PAX_AIR(
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
      .builder()
      .master("local[*]")
      .appName("databricks-sample1")
      .config("spark.default.parallelism", 4)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .getOrCreate()
    spark.conf.set("spark.sql.debug.maxToStringFields", 10000)

    import spark.implicits._

    val mainDF = Seq(
      RES_PAX_AIR("1A",
        "DDVR08",
        "2020-06-23",
        1,
        "2020-06-23 00:00:00",
        "2099-12-31 00:00:00",
        1,
        1,
        ""),
      RES_PAX_AIR("1B",
        "DDVR09",
        "2020-06-23",
        1,
        "2020-06-23 00:00:00",
        "2099-12-31 00:00:00",
        1,
        1,
        ""),
      RES_PAX_AIR("1C",
        "DDVR10",
        "2020-06-24",
        1,
        "2020-06-24 00:00:00",
        "2099-12-31 00:00:00",
        1,
        1,
        ""),
      RES_PAX_AIR("1D",
        "DDVR11",
        "2020-06-24",
        1,
        "2020-06-24 00:00:00",
        "2099-12-31 00:00:00",
        1,
        1,
        ""),
      RES_PAX_AIR("1A",
        "DDVR12",
        "2020-06-24",
        1,
        "2020-06-24 00:00:00",
        "2099-12-31 00:00:00",
        1,
        1,
        "")
    ).toDF()

//    if(!DeltaTable.isDeltaTable("/tmp/abc123/"))
//        DeltaTable.convertToDelta(spark,"/tmp/abc123/")

    if(DeltaTable.isDeltaTable("/tmp/abc123/")) {
      val existingTable: DeltaTable = DeltaTable.forPath("/tmp/abc123/")
      println("merge happening")
      existingTable.as("main").merge(
        mainDF.as("newData"),
        "main.PNR_ENVLP_NUM = newData.PNR_ENVLP_NUM"
      ).whenMatched
        .updateAll().whenNotMatched.insertAll()
        .execute()
    }else{
      mainDF.write.format("delta").mode("overwrite").save("/tmp/abc123/")
    }


  }
}
