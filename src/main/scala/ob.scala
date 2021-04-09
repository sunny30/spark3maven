package oop

import scala.collection.mutable.ListBuffer
import scala.io.Source

object ob {

  def main(args: Array[String]): Unit = {
    val str =
      """---map table t1ewe
        |select a,b,c from t
        |---map table fgdgf
        |select a,b,c
        |from t2
        |""".stripMargin
    println(str)

    val splitseq = str.split("---map table .*\n")

    println(splitseq)
    val path = "/Users/sharadsingh/Dev/scalaSparkEx/src/main/scala/oop/hql.txt"
    val hql = readFile(path)
    val refinedHql = cleanupWithHiveConstructs(hql)
    val dropTableList = createDropTableList(refinedHql)
    val createTableList = createCreateTableList(refinedHql)
    val hqls = refinedHql.split("-- Map Step .*\n")
    val sparkSQLs = hqls.map(hql => cleanupHQL(hql)).seq
    val sparkCreateSqls = sparkSQLs.filter(x=>x.startsWith("CREATE TABLE")).seq
    val targetSQLMap = collectTargetSQLMap(sparkCreateSqls)
    val viewsMap = convertAllCreateTableToTempViews(targetSQLMap)

    hqls.length
  }


  def readFile(filePath: String): String = {
    val hql = Source.fromFile(filePath).mkString
    hql
  }

  def replaceSeesionIdInHql(hql: String) = {
    val hqlWithoutSessionId = hql.replaceAll("##!sessionid##", "")
    hqlWithoutSessionId
  }

  def replaceIBECToHiveConstructs(hql: String): String = {
    val hqlWithHiveConstruct = hql.replace("IBECTrim", "trim")
    hqlWithHiveConstruct
  }

  def replaceIBEC42MonthToHiveConstruct(hql: String): String = {
    val hql42monthWithHiveConstructs = hql.replace("Date ##startdate42months##", "add_months(trunc(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'),'MM'),-43)")
    hql42monthWithHiveConstructs
  }

  def replaceIBEC36MonthToHiveConstruct(hql: String): String = {
    val hql36monthWithHiveConstructs = hql.replace("Date ##startdate36months##", "add_months(trunc(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'),'MM'),-39)")
    hql36monthWithHiveConstructs
  }

  def createDropTableList(hql: String): Seq[String] = {
    val lines = hql.split("\n")
    val dropTableStatementList: Seq[String] = lines.filter(i => i.startsWith("DROP TABLE")).seq
    dropTableStatementList.map(x => {
      val tablename = x.replace("DROP TABLE ", "")

      tablename.replace("\n", "")
    }).seq

  }

  def createCreateTableList(hql: String): Seq[String] = {
    val lines = hql.split("\n")
    val dropTableStatementList: Seq[String] = lines.filter(i => i.startsWith("CREATE TABLE")).seq
    dropTableStatementList.map(x => {
      val tablename = x.replace("CREATE TABLE ", "")

      tablename.replace("\n", "")
    }).seq
  }

  def cleanupHQL(hql: String): String = {
    val hqlLines = hql.split("\n")
    val validHql = hqlLines.map(line => {
      if (!line.startsWith("--") && !line.startsWith("DROP TABLE "))
        line
      else {
        ""
      }
    }).mkString("\n")

    validHql

  }

  def cleanupWithHiveConstructs(hql: String): String = {
    val hqlWithoutSession = replaceSeesionIdInHql(hql)
    val hqlWithout36Months = replaceIBEC36MonthToHiveConstruct(hqlWithoutSession)
    val hqlWithout42Months = replaceIBEC42MonthToHiveConstruct(hqlWithout36Months)
    val hqlWithHiveTrim = replaceIBECToHiveConstructs(hqlWithout42Months)
    hqlWithHiveTrim
  }

  def createTargetsSQLMap(hql: String): (String, String) = {
    val keywords = hql.split(" ")
    if (keywords(0).equalsIgnoreCase("create") && keywords(1).equalsIgnoreCase("table")) {
      (keywords(2).replaceAll("\n", ""), hql)
    } else {
      ("", "")
    }
  }

  def createTargetOrderedList(hqls:Seq[String]):Seq[String]={
    hqls.map(hql=>{
      val sqlKeyWords = hql.split(" ")
      sqlKeyWords(2).replace("\n","")
    })
  }

  def collectTargetSQLMap(hqls:Seq[String]):Map[String,String]={
    val resultMap = hqls.map(hql=>{
      createTargetsSQLMap(hql)
    }).toMap
    resultMap
  }

  def convertAllCreateTableToTempViews(targetMap:Map[String,String]):Map[String,String]={
    val viewsMap = targetMap.map(x=>{
      (x._1,x._2.replaceAll("CREATE TABLE","CREATE TEMP VIEW"))
    }).toMap
    viewsMap
  }


  def getTargetViewMap(filePath:String):Map[String,String]={
    val hql = readFile(filePath)
    val refinedHql = cleanupWithHiveConstructs(hql)
    val dropTableList = createDropTableList(refinedHql)
    val createTableList = createCreateTableList(refinedHql)
    val hqls = refinedHql.split("-- Map Step .*\n")
    val sparkSQLs = hqls.map(hql => cleanupHQL(hql)).seq
    val sparkCreateSqls = sparkSQLs.filter(x=>x.startsWith("CREATE TABLE")).seq
    val targetSQLMap = collectTargetSQLMap(sparkCreateSqls)
    val viewsMap = convertAllCreateTableToTempViews(targetSQLMap)
    viewsMap
  }

  def orderedTargetList(filePath:String):Seq[String]={
    val hql = readFile(filePath)
    val refinedHql = cleanupWithHiveConstructs(hql)
    val hqls = refinedHql.split("-- Map Step .*\n")
    val sparkSQLs = hqls.map(hql => cleanupHQL(hql)).seq
    val sparkCreateSqls = sparkSQLs.filter(x=>x.startsWith("CREATE TABLE")).seq
    createTargetOrderedList(sparkCreateSqls)
  }




}

