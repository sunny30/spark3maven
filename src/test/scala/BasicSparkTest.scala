import com.holdenkarau.spark.testing.DataFrameSuiteBase
import oop.ob.cleanupWithHiveConstructs
import org.scalatest.FunSuite
import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION

import scala.io.Source


class BasicSparkTest extends FunSuite  with DataFrameSuiteBase{

  override def conf:                       SparkConf = super.conf.set(CATALOG_IMPLEMENTATION.key, "hive")
  //conf.set("spark.sql.warehouse.dir","/tmp/")
  implicit override def enableHiveSupport: Boolean   = true

  test("first"){

   // spark.conf.set("spark.sql.warehouse.dir","/tmp/")
    spark.sql("CREATE TABLE IF NOT EXISTS t6(id long)")
    spark.sql("CREATE TABLE IF NOT EXISTS t7(id long)")

    val q = spark.sql("select * from t6 where id not in (select id from t7 where id is not null)")

    println(q.queryExecution.logical.prettyJson)

  }



  test("second"){

    // spark.conf.set("spark.sql.warehouse.dir","/tmp/")
    spark.sql("CREATE TABLE IF NOT EXISTS t8(A long,X long)")
    spark.sql("CREATE TABLE IF NOT EXISTS t9(A long,X long)")

    val q = spark.sql("SELECT A FROM t8 WHERE EXISTS (SELECT A FROM t9 WHERE t8.X = t9.X)")

    println(q.queryExecution.logical.prettyJson)

  }

  test("third"){

  }

  test("fourth"){
    spark.sql("create table if not exists tt3(A bigint,B smallint)" ) ;
    spark.sql("create table if not exists tt4(A bigint,B smallint)" ) ;
    val df = spark.sql("create temp view ttview as select aa.A,aa.B,bb.A,bb.B from tt3 aa inner join tt4 bb on aa.A=bb.A")
    df.explain(true)
  }

  test("fifth"){
    val df = spark.sql("select add_months(trunc('2015-02-01','MM'),-2)")
    df.show()
  }

  test("sixth"){
    val hiveql =
      """--map values
        ||select add_months(trunc('2015-02-01','MM'),-2)"""

    val df = spark.sql(hiveql)

  }

  test("seventh"){

    spark.sql("create table if not exists ##!sessionid##tt3(A bigint,B smallint)" ) ;
  }

  test("Eight"){
   // spark.sql("SELECT (to_date(Trunc(date_add('mm', -43, From_UnixTime(Unix_Timestamp(), 'yyyy-MM-dd')), 'MM')), 'yyyy-MM-dd')")

    val df = spark.sql("select add_months(trunc(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'),'MM'),-2)")
    df.show()
  }

  test("Ninth"){
    spark.sql("create table if not exists tt5(A bigint,B smallint)" ) ;
    val df = spark.sql("select A,B from tt5 where '2020-10-30'>= add_months(trunc(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'),'MM'),-2)")
    df.explain(true)
  }

  test("Tenth"){
    spark.sql("create table if not exists tt8(A bigint,B smallint)" ) ;
    spark.sql("create table if not exists tt9(C bigint,D smallint)" ) ;
    spark.sql("create temp view ttview2 as select aa.A,aa.B,bb.C,bb.D from tt8 aa inner join tt9 bb on aa.A=bb.C")
    val df = spark.sql("create table ttfinal as select * from ttview2")
    df.explain(true)

  }

  test("eleventh"){
    val str =
      """CREATE TABLE testschema(
        cstone_feed_key       	bigint      	,
        |cstone_last_updatetm  	string      	,
        |cm11  	bigint      	,
        |cm15  	bigint      	,
        |cm13  	bigint)
        |""".stripMargin
    spark.sql(str)
  }

  test("twelve"){
    spark.sql("create database cstonedb3")
    val filePath = "/Users/sharadsingh/Documents/spark3maven/src/main/resources/cstonedb3.risk_pers_acct"
    val str = Source.fromFile(filePath).mkString
    spark.sql(str)

  }
  test("thirteenth") {
    spark.sql("create database cobrand_ref")
    val filePath = "/Users/sharadsingh/Documents/spark3maven/src/main/resources/cobrand_ref.cbpr_product_master"
    val str = Source.fromFile(filePath).mkString
    spark.sql(str)
  }

  test("forteenth"){
    spark.sql("create database cobrand_ref")
    spark.sql("create database cstonedb3")
    spark.sql("drop table IF EXISTS cobrand_ref.cbpr_product_master")
    spark.sql("drop table IF EXISTS cstonedb3.cmdl_card_master")
    spark.sql("drop table IF EXISTS cstonedb3.crt_currency")
    spark.sql("drop table IF EXISTS cstonedb3.gen_card_acct")
    spark.sql("drop table IF EXISTS cstonedb3.gms_global_cm_demog")
    spark.sql("drop table IF EXISTS cstonedb3.gstar_account_finance_info")
    spark.sql("drop table IF EXISTS cstonedb3.gstar_market_identifier")
    spark.sql("drop table IF EXISTS cstonedb3.risk_pers_acct")
    spark.sql("drop table IF EXISTS cstonedb3.risk_pers_acct_hist")
    spark.sql("drop table IF EXISTS cstonedb3.risk_pers_acct_timeseries")
    spark.sql("drop table IF EXISTS cstonedb3.wwfis_fraud_trans")
    spark.sql("drop table IF EXISTS cstonedb3.risk_new_acct")
    spark.sql("drop table IF EXISTS cstonedb3.gstar_card_details")


    val filesList = Seq(
        "/Users/sharadsingh/Documents/spark3maven/src/main/resources/cobrand_ref.cbpr_product_master",
        "/Users/sharadsingh/Documents/spark3maven/src/main/resources/cstonedb3.cmdl_card_master",
        "/Users/sharadsingh/Documents/spark3maven/src/main/resources/cstonedb3.crt_currency",
        "/Users/sharadsingh/Documents/spark3maven/src/main/resources/cstonedb3.gen_card_acct",
        "/Users/sharadsingh/Documents/spark3maven/src/main/resources/cstonedb3.gms_global_cm_demog",
        "/Users/sharadsingh/Documents/spark3maven/src/main/resources/cstonedb3.gstar_account_finance_info",
        "/Users/sharadsingh/Documents/spark3maven/src/main/resources/cstonedb3.gstar_card_details",
        "/Users/sharadsingh/Documents/spark3maven/src/main/resources/cstonedb3.gstar_market_identifier",
        "/Users/sharadsingh/Documents/spark3maven/src/main/resources/cstonedb3.risk_new_acct",
        "/Users/sharadsingh/Documents/spark3maven/src/main/resources/cstonedb3.risk_pers_acct",
        "/Users/sharadsingh/Documents/spark3maven/src/main/resources/cstonedb3.risk_pers_acct_hist",
        "/Users/sharadsingh/Documents/spark3maven/src/main/resources/cstonedb3.risk_pers_acct_timeseries",
        "/Users/sharadsingh/Documents/spark3maven/src/main/resources/cstonedb3.wwfis_fraud_trans",
        "/Users/sharadsingh/Documents/spark3maven/src/main/resources/cstonedb3.cmdl_card_master_intl"
      )
    val path = "/Users/sharadsingh/Dev/scalaSparkEx/src/main/scala/oop/hql.txt"
    filesList.map(x=>spark.sql(Source.fromFile(x).mkString))
    val orderedList = oop.ob.orderedTargetList(path)
    val mp = oop.ob.getTargetViewMap(path)
    orderedList.map(tableName=>{
      spark.sql(mp.get(tableName).get)
    })
  }


  test("sparkschema"){
     val p =spark.sql("create database cobrand_ref")
     val p1= spark.sql("create database cstonedb3")
    val filesList = Seq(
      "/Users/sharadsingh/Documents/spark3maven/src/main/resources/cobrand_ref.cbpr_product_master",
      "/Users/sharadsingh/Documents/spark3maven/src/main/resources/cstonedb3.cmdl_card_master",
      "/Users/sharadsingh/Documents/spark3maven/src/main/resources/cstonedb3.crt_currency",
      "/Users/sharadsingh/Documents/spark3maven/src/main/resources/cstonedb3.gen_card_acct",
      "/Users/sharadsingh/Documents/spark3maven/src/main/resources/cstonedb3.gms_global_cm_demog",
      "/Users/sharadsingh/Documents/spark3maven/src/main/resources/cstonedb3.gstar_account_finance_info",
      "/Users/sharadsingh/Documents/spark3maven/src/main/resources/cstonedb3.gstar_card_details",
      "/Users/sharadsingh/Documents/spark3maven/src/main/resources/cstonedb3.gstar_market_identifier",
      "/Users/sharadsingh/Documents/spark3maven/src/main/resources/cstonedb3.risk_new_acct",
      "/Users/sharadsingh/Documents/spark3maven/src/main/resources/cstonedb3.risk_pers_acct",
      "/Users/sharadsingh/Documents/spark3maven/src/main/resources/cstonedb3.risk_pers_acct_hist",
      "/Users/sharadsingh/Documents/spark3maven/src/main/resources/cstonedb3.risk_pers_acct_timeseries",
      "/Users/sharadsingh/Documents/spark3maven/src/main/resources/cstonedb3.wwfis_fraud_trans",
      "/Users/sharadsingh/Documents/spark3maven/src/main/resources/cstonedb3.cmdl_card_master_intl"
    )
    filesList.map(x=>spark.sql(Source.fromFile(x).mkString))

   // val p =spark.sql("create database cobrand_ref").schema.json
   // val p1= spark.sql("create database cstonedb3").schema.json
    val p2=spark.sql("select * from cobrand_ref.cbpr_product_master").schema.json
    val p3 =spark.sql("select * from cstonedb3.cmdl_card_master").schema.json
    val p4 = spark.sql("select * from  cstonedb3.crt_currency").schema.json
    val p5 = spark.sql("select * from cstonedb3.gen_card_acct").schema.json
    val p6 =spark.sql("select * from cstonedb3.gms_global_cm_demog").schema.json
    val p7 =spark.sql("select * from cstonedb3.gstar_account_finance_info").schema.json
    val p8= spark.sql("select * from cstonedb3.gstar_market_identifier").schema.json
    val p9 = spark.sql("select * from  cstonedb3.risk_pers_acct").schema.json
    val p14 = spark.sql("select * from cstonedb3.risk_pers_acct_hist").schema.json
    val p10 = spark.sql("select * from cstonedb3.risk_pers_acct_timeseries").schema.json
    val p11 = spark.sql("select * from cstonedb3.wwfis_fraud_trans").schema.json
    val p12 = spark.sql("select * from cstonedb3.risk_new_acct").schema.json
    val p13 = spark.sql("select * from cstonedb3.gstar_card_details").schema.json
    val p15 = spark.sql("select * from cstonedb3.cmdl_card_master_intl")

    println("Done")



  }


}
