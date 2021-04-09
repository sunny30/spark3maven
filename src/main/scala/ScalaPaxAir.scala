import io.delta.tables.DeltaTable
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ScalaPaxAir {

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

    val keyColumns = List("RES_SYS_ID", "PNR_REC_LOC_ID")

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

    val seqOfSWARecords = Seq(
      RES_PAX_AIR("1A",
        "DDVR08",
        "2020-06-23",
        3,
        "2020-06-24 00:00:00",
        "2099-12-31 00:00:00",
        1,
        1,
        ""),
      RES_PAX_AIR("1A",
        "DDVR08",
        "2020-06-23",
        4,
        "2020-06-26 00:00:00",
        "2099-12-31 00:00:00",
        1,
        1,
        ""),
      RES_PAX_AIR("1C",
        "DDVR10",
        "2020-06-24",
        5,
        "2020-06-26 00:00:00",
        "2099-12-31 00:00:00",
        1,
        1,
        ""),
      RES_PAX_AIR("1C",
        "DDVR10",
        "2020-06-24",
        6,
        "2020-06-27 00:00:00",
        "2099-12-31 00:00:00",
        1,
        1,
        ""),
      RES_PAX_AIR("1C",
        "DDVR10",
        "2020-06-24",
        7,
        "2020-06-28 00:00:00",
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
        "")
    )

    val cdcDF = seqOfSWARecords.toDF()
    val cdcDF_with_merge =
      cdcDF.withColumn("mergeKey", concat(keyColumns.map(cdcDF.col): _*))
    val cdc_ordered_df = window_df(cdcDF_with_merge)
    cdc_ordered_df.show()
    val updateColumns: Array[String] = mainDF.columns

    mainDF.write.format("delta").mode("overwrite").save("/tmp/pax-air")

    scd2Merge("/tmp/pax-air", cdc_ordered_df)
  }

  def scd2Merge(existingTablePath: String, updatesDF: DataFrame): Unit = {

    val existingTable: DeltaTable = DeltaTable.forPath(existingTablePath)
    val keyColumns = List("RES_SYS_ID", "PNR_REC_LOC_ID")
    val existing_tempDF = existingTable.toDF
    val existingDF = existing_tempDF.withColumn(
      "mergeKey",
      concat(keyColumns.map(existing_tempDF.col): _*))

    val minFlagColumn = "MIN_EFF_FM_FLAG"
    val maxFlagColumn = "MAX_EFF_TO_FLAG"
    val updateColumns: Array[String] = updatesDF.columns
    val flagN = 0
    val toTimeColumn = "EFF_TO_CENT_TZ"
    val deltaWindowFirstColumn = "FIRST_EFF_FM_CENT_TZ"

    val scdHistoricColumns = List("PNR_ENVLP_NUM")

    val rowsToUpdate = updatesDF
      .alias("left")
      .join(existingDF, keyColumns) // join on key columns
      .where(existingDF
        .col(s"$maxFlagColumn") === lit(1) && // Only consider TopOfStack (TOS)
        scdHistoricColumns
          .map { c =>
            existingDF.col(c) =!= updatesDF.col(c)
          }
          .reduce((x, y) => x || y))
      .select(updateColumns.map(updatesDF.col): _*)
      .withColumn(s"$minFlagColumn", lit(0))

    println("rows to insert data")
    rowsToUpdate.show()
    //manipulation to make rows insert marking mergeKey null so they can get insert in when not matched column,select to make union happen or maintaining the order
    val rowstoInsertTemp = rowsToUpdate.withColumn("mergeKey", lit(null))
    val rowsToInsert = rowstoInsertTemp.select(rowstoInsertTemp.columns.map(expr(_)): _*)
    rowsToInsert.show()

    val dedupUpdateDF = dedupSort(
      typeToKeep = "first",
      groupByColumns = List(col("RES_SYS_ID"), col("PNR_REC_LOC_ID")),updatesDF
    )
    val stagedUpdatesDF = rowsToInsert // new rows
      .union(dedupUpdateDF.withColumn("mergeKey", concat(keyColumns.map(updatesDF.col): _*))) // updates will come from within these

    println("staged data")
    stagedUpdatesDF.show()

    existingTable
      .as("main")
      .merge(
        stagedUpdatesDF.as("cdc"),
        concat(keyColumns.map(existingDF.col): _*) === stagedUpdatesDF(
          "mergeKey")
      )
      .whenMatched(
        existingDF.col(s"$maxFlagColumn") === lit(1) && (
          scdHistoricColumns
            .map { scdCol =>
              existingDF.col(scdCol) =!= stagedUpdatesDF.col(scdCol)
            }
            .reduce((c1, c2) => c1 || c2)
          )
      )
      .updateExpr(
        Map(
          s"$maxFlagColumn" -> s"$flagN",
          s"$toTimeColumn" -> s"cdc.$deltaWindowFirstColumn"
        )
      )
      .whenNotMatched()
      .insertAll()
      .execute()

    existingTable.toDF.show()

  }

  def window_df(in: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy("mergeKey").orderBy("PNR_ENVLP_NUM")
    val out = in
      .withColumn("MAX_EFF_TO_CENT_TZ",
        max(col("EFF_TO_CENT_TZ")).over(windowSpec))
      .withColumn("FIRST_EFF_FM_CENT_TZ",
        first(col("EFF_FM_CENT_TZ")).over(windowSpec))
      .withColumn(
        "PREV_EFF_TO_CENT_TZ",
        lead(col("EFF_FM_CENT_TZ"), 1, "2099-12-31").over(windowSpec))
      .withColumn("MAX_EFF_FM_FLAG",
        when(col("PREV_EFF_TO_CENT_TZ") === col("EFF_TO_CENT_TZ"),
          lit(1)).otherwise(lit(0)))//.when(col(""),lit(1))multiple when are possible
      .drop("EFF_TO_CENT_TZ")
      .withColumnRenamed("PREV_EFF_TO_CENT_TZ", "EFF_TO_CENT_TZ")
      .drop("MAX_EFF_TO_CENT_TZ").drop("mergeKey")

    out

  }

  //this utility came because multiple records of same key is not allowed at the time of merge by delta
  def dedupSort(typeToKeep: String,
                groupByColumns: List[Column], dataFrame: DataFrame): DataFrame = {
    val window = Window
      .partitionBy(groupByColumns: _*)
      .orderBy(lit(1))
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val windowForCount = Window
      .partitionBy(groupByColumns: _*)
      .orderBy(lit(1))
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    typeToKeep match {
      case "any" ⇒ dataFrame.dropDuplicates(groupByColumns.map(_.toString()))
      case "first" ⇒
        val dataFrameWithRowNumber =
          dataFrame.withColumn("row_number", row_number().over(window))
        dataFrameWithRowNumber
          .filter(col("row_number") === lit(1))
          .drop("row_number")

      case "last" ⇒
        val dataFrameWithRowNumber = dataFrame
          .withColumn("row_number", row_number().over(window))
          .withColumn("count", count("*").over(windowForCount))
        dataFrameWithRowNumber
          .filter(col("row_number") === col("count"))
          .drop("row_number")
          .drop("count")

      case "unique-only" ⇒
        val dataFrameWithCount =
          dataFrame.withColumn("count", count("*").over(windowForCount))
        dataFrameWithCount.filter(col("count") === lit(1)).drop("count")
    }

  }
}
