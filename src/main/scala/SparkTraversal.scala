import org.apache.spark.sql.SparkSession

object SparkTraversal{

  case class RES_PAX_AIR
  (
    RES_SYS_ID: String,
    PNR_REC_LOC_ID: String,
    PNR_CRE_DT_RRE_CENT_TZ: String,
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
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .config("spark.sql.warehouse.dir", "/Users/sharadsingh/Documents/spark3maven/warehouse")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").enableHiveSupport()
      .getOrCreate()




    //spark.sql("CREATE TABLE IF NOT EXISTS t1(id long)")
    //spark.sql("CREATE TABLE IF NOT EXISTS t2(id long)")

   // val q = spark.sql("INSERT INTO TABLE t2 SELECT * from t1 LIMIT 100")
    val q = spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS spark_tests.s3_table_1 (key INT, value STRING) STORED AS PARQUET LOCATION 'tmp/s3_table_1'")
    println(q.queryExecution.logical.prettyJson)
  }
}
