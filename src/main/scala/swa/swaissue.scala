package swa

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object swaissue {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("databricks-sample1")
      .config("spark.default.parallelism", 4)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .getOrCreate()

    val nestedDF = spark.read
      .format("json")
      .option("multiLine", true).option("inferSchema", "true")
      .load(
        "dbfs:/mnt/swa-poc/bookings/reservations/feeds/2020/6/24/RES/0050054003951_RES_Y0DZT4_2020-06-24_1.json"
      )

    val flatDF = flatSchema(spark,nestedDF)
    val reformatDF = reformat(spark,flatDF)
    val aggDF = agg(spark,reformatDF)
    targetStore(spark,aggDF)




  }


  def flatSchema(sparkSession: SparkSession,in:DataFrame):DataFrame={
    val flattened = in
      .withColumn(
        "CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem",
        explode(col("CdsFeed.feedBody.Reservation.passengerList.passenger.reservationItemList.reservationItem"))
      )
      .withColumn("CdsFeed_feedBody_Reservation_boundSegmentList_boundSegment",
        explode(col("CdsFeed.feedBody.Reservation.boundSegmentList.boundSegment"))
      )
      .withColumn(
        "CdsFeed_feedBody_Reservation_boundSegmentList_boundSegment_boundDetails",
        explode(col("CdsFeed_feedBody_Reservation_boundSegmentList_boundSegment.boundDetails"))
      )
      .withColumn(
        "CdsFeed_feedBody_Reservation_airFareList_airFare_airFarePricingList_airFarePricing",
        explode(col("CdsFeed.feedBody.Reservation.airFareList.airFare.airFarePricingList.airFarePricing"))
      )
    val out = flattened.select(
      col("CdsFeed.feedBody.Reservation.airFareList.airFare.airFareSequenceID").as("airFareSequenceID"),
      col(
        "CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem.departureInfo.departureDateTimeUtc"
      ).as("departureDateTimeUtc"),
      col(
        "CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem.bookingCreationInfo.bookingCreationDate"
      ).as("bookingCreationDate"),
      col(
        "CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem.departureInfo.departureAirportCode"
      ).as("departureAirportCode"),
      col("CdsFeed.feedBody.Reservation.passengerList.passenger.reservationSystemCustomerID")
        .as("reservationSystemCustomerID"),
      col(
        "CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem.marketingCarrierCode"
      ).as("marketingCarrierCode"),
      col(
        "CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem.bookingCreationInfo.bookingCreationTimeZoneCode"
      ).as("bookingCreationTimeZoneCode"),
      col("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem.segmentStatusCode")
        .as("segmentStatusCode"),
      col(
        "CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem.originalSegmentStatusCode"
      ).as("originalSegmentStatusCode"),
      col(
        "CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem.passengerCountOnSegment"
      ).as("passengerCountOnSegment"),
      col("CdsFeed.feedBody.Reservation.airFareList.airFare.passengerTypeCode").as("passengerTypeCode"),
      col("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem.classOfServiceCode")
        .as("classOfServiceCode"),
      col("CdsFeed_feedBody_Reservation_boundSegmentList_boundSegment_boundDetails.extendedRoutingKey")
        .as("extendedRoutingKey"),
      col("CdsFeed_feedBody_Reservation_boundSegmentList_boundSegment_boundDetails.originAirportCode")
        .as("originAirportCode"),
      col(
        "CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem.departureInfo.departureDateTime"
      ).as("departureDateTime"),
      col("CdsFeed_feedBody_Reservation_boundSegmentList_boundSegment_boundDetails.direction").as("direction"),
      col("CdsFeed_feedBody_Reservation_boundSegmentList_boundSegment_boundDetails.boundItemSequenceNumber")
        .as("boundItemSequenceNumber"),
      col("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem.reservationItemID")
        .as("reservationItemID"),
      col(
        "CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem.marketingFlightNumber"
      ).as("marketingFlightNumber"),
      col("CdsFeed.feedBody.Reservation.pnrCreateDate").as("pnrCreateDate"),
      col(
        "CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem.arrivalInfo.arrivalAirportCode"
      ).as("arrivalAirportCode"),
      col("CdsFeed_feedBody_Reservation_airFareList_airFare_airFarePricingList_airFarePricing.fareBasisPrimaryCode")
        .as("fareBasisPrimaryCode"),
      col("CdsFeed.feedBody.Reservation.reservationSystemCode").as("reservationSystemCode"),
      col("CdsFeed.feedBody.Reservation.lastUpdateTimestamp").as("lastUpdateTimestamp"),
      col("CdsFeed.feedBody.Reservation.pnrReservationSystemSequenceID").as("pnrReservationSystemSequenceID"),
      col("CdsFeed.feedBody.Reservation.recordLocatorID").as("recordLocatorID"),
      col(
        "CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem.reservationSystemPassengerReservationItemID"
      ).as("reservationItem_reservationSystemPassengerReservationItemID"),
      col(
        "CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem.arrivalInfo.arrivalDateTime"
      ).as("arrivalDateTime"),
      col(
        "CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem.arrivalInfo.arrivalDateTimeUtc"
      ).as("arrivalDateTimeUtc"),
      col("CdsFeed.feedBody.Reservation.passengerList.passenger.ssrList.serviceRequest.freeText").as("freeText"),
      col("CdsFeed.feedBody.Reservation.passengerList.passenger.ssrList.serviceRequest.specialServiceRequestTypeCode")
        .as("specialServiceRequestTypeCode")
    )

    out
  }

  def reformat(sparkSession: SparkSession,in:DataFrame):DataFrame={
    val out = in.select(
      coalesce(col("reservationSystemCode"),         lit("-")).as("RES_SYS_ID"),
      coalesce(col("recordLocatorID"),               lit("-")).as("PNR_REC_LOC_ID"),
      substring(col("pnrCreateDate"),                1, 10).cast(DateType).as("PNR_CRE_DT"),
      substring(col("arrivalDateTime"),              1, 10).cast(DateType).as("SEG_ARR_DT"),
      coalesce(col("airFareSequenceID"),             lit("-")).as("FARE_OTHR_TATO_NUM"),
      coalesce(col("fareBasisPrimaryCode"),          lit("-")).as("FARE_BSIS_CDE"),
      coalesce(col("marketingCarrierCode"),          lit("-")).as("MKTG_CARR_CDE"),
      coalesce(col("bookingCreationTimeZoneCode"),   lit("-")).as("SEG_BKNG_H2MI"),
      coalesce(col("segmentStatusCode"),             lit("-")).as("SEG_ACTN_CDE"),
      coalesce(col("originalSegmentStatusCode"),     lit("-")).as("ORGL_SEG_ACTN_CDE"),
      coalesce(col("passengerCountOnSegment"),       lit("-")).as("PAX_CT"),
      coalesce(col("passengerTypeCode"),             lit("-")).as("EXTRA_SEAT_CT"),
      coalesce(col("classofServiceCode"),            lit("-")).as("BKNG_CLSS_CDE"),
      coalesce(col("extendedRoutingKey"),            lit("-")).as("TRIP_ID"),
      coalesce(col("originAirportCode"),             lit("-")).as("TRIP_ORIG_ARPT_CDE"),
      coalesce(col("arrivalAirportCode"),            lit("-")).as("TRIP_DEST_ARPT_CDE"),
      coalesce(col("extendedRoutingKey"),            lit("-")).as("ITIN_TRIP_SEQ_NUM"),
      substring(col("departureDateTime"),            1, 10).cast(DateType).as("TRIP_DEP_DT"),
      coalesce(col("direction"),                     lit("-")).as("ITIN_PART_TYPE_CDE"),
      coalesce(col("boundItemSequenceNumber"),       lit("-")).as("TRIP_SEG_CT"),
      coalesce(col("reservationItemID"),             lit("-")).as("CONN_TYPE_CDE"),
      coalesce(col("freeText"),                      lit("-")).as("PAX_CHK_IN_ID"),
      coalesce(col("freeText"),                      lit("-")).as("ACPT_CHNL_ORIG_CDE"),
      coalesce(col("freeText"),                      lit("-")).as("ACPT_CHNL_TYPE_CDE"),
      coalesce(col("freeText"),                      lit("-")).as("BRDG_PASS_GRP_CDE"),
      coalesce(col("freeText"),                      lit("-")).as("BRDG_PASS_GRP_SEQ_NUM"),
      coalesce(col("freeText"),                      lit("-")).as("SEAT_NUM_ID"),
      substring(col("freeText"),                     1, 10).cast(DateType).as("PAX_BRD_DT"),
      coalesce(col("freeText"),                      lit("-")).as("PAX_BRD_H2MI"),
      coalesce(col("freeText"),                      lit("-")).as("CHK_BAG_STAT_CDE"),
      coalesce(col("freeText"),                      lit("-")).as("APIS_QCK_QRY_STAT_CDE"),
      coalesce(col("fareBasisPrimaryCode"),          lit("-")).as("BUS_SEL_FLAG"),
      coalesce(col("specialServiceRequestTypeCode"), lit("-")).as("EARLY_BIRD_FLAG"),
      lit("1").as("DOM_INTL_CDE"),
      lit("1").as("LATEST_JOB_ID"),
      coalesce(col("freeText"), lit("-")).as("CHK_BAG_CT"),
      lit('-').as("PNR_PAX_SEQ_NUM"),
      substring(col("departureDateTime"),          1, 10).cast(DateType).as("SEG_DEP_DT"),
      coalesce(col("reservationSystemCustomerID"), lit("-")).as("RES_SYS_PAX_ID"),
      lit('1').as("ORIGINAL_JOB_ID"),
      coalesce(col("marketingCarrierCode"), lit("-")).as("CDE_SHR_TYPE_CDE"),
      coalesce(col("arrivalDateTimeUtc"),   lit("-")).as("SEG_ARR_UTC_OFST_H2MI"),
      lit("2099-12-31 00:00:00").cast(TimestampType).as("EFF_TO_CENT_TZ"),
      coalesce(col("marketingFlightNumber"),     lit("-")).as("OPNG_FLT_NUM"),
      coalesce(col("boundItemSequenceNumber"),   lit("-")).as("TRIP_SEG_SEQ_NUM"),
      coalesce(col("marketingFlightNumber"),     lit("-")).as("MKTG_FLT_NUM"),
      coalesce(col("departureDateTime"),         lit("-")).as("SEG_DEP_H2MI"),
      regexp_replace(col("lastUpdateTimestamp"), "T", " ").cast(TimestampType).as("EFF_FM_CENT_TZ"),
      coalesce(col("marketingCarrierCode"),      lit("-")).as("OPNG_CARR_CDE"),
      coalesce(col("passengerTypeCode"),         lit("-")).as("PAX_LAP_INF_FLAG"),
      coalesce(col("fareBasisPrimaryCode"),      lit("-")).as("NONREV_TYPE_CDE"),
      coalesce(col("departureDateTime"),         lit("-")).as("ITIN_SEG_SEQ_NUM"),
      lit("1").as("SEG_DEST_ARPT_CDE"),
      coalesce(col("departureAirportCode"), lit("-")).as("SEG_ORIG_ARPT_CDE"),
      lit("1").as("OPNL_INFO_COPY_FLAG"),
      substring(col("bookingCreationDate"), 1, 10).cast(DateType).as("SEG_BKNG_DT"),
      coalesce(col("arrivalDateTime"),      lit("-")).as("SEG_ARR_H2MI"),
      coalesce(col("departureDateTimeUtc"), lit("-")).as("SEG_DEP_UTC_OFST_H2MI"),
      col("pnrReservationSystemSequenceID").as("PNR_ENVLP_NUM"),
      lit("RES_PAX_AIR").as("TBL_NME"),
      lit("2020-07-06 14:47:09").as("CUR_TS"),
      lit(1).as("MIN_EFF_FM_FLAG"),
      lit(1).as("MAX_EFF_TO_FLAG")
    )

    out
  }

  def agg(sparkSession: SparkSession,in:DataFrame):DataFrame={
    val dfGroupBy = in.groupBy(
      col("RES_SYS_ID").as("RES_SYS_ID_R"),
      col("PNR_REC_LOC_ID").as("PNR_REC_LOC_ID_R"),
      col("PNR_CRE_DT").as("PNR_CRE_DT_R"),
      col("SEG_ARR_DT").as("SEG_ARR_DT_R"),
      col("FARE_OTHR_TATO_NUM").as("FARE_OTHR_TATO_NUM_R"),
      col("FARE_BSIS_CDE").as("FARE_BSIS_CDE_R"),
      col("MKTG_CARR_CDE").as("MKTG_CARR_CDE_R"),
      col("SEG_BKNG_H2MI").as("SEG_BKNG_H2MI_R"),
      col("SEG_ACTN_CDE").as("SEG_ACTN_CDE_R"),
      col("ORGL_SEG_ACTN_CDE").as("ORGL_SEG_ACTN_CDE_R"),
      col("PAX_CT").as("PAX_CT_R"),
      col("EXTRA_SEAT_CT").as("EXTRA_SEAT_CT_R"),
      col("BKNG_CLSS_CDE").as("BKNG_CLSS_CDE_R"),
      col("TRIP_ID").as("TRIP_ID_R"),
      col("TRIP_ORIG_ARPT_CDE").as("TRIP_ORIG_ARPT_CDE_R"),
      col("TRIP_DEST_ARPT_CDE").as("TRIP_DEST_ARPT_CDE_R"),
      col("ITIN_TRIP_SEQ_NUM").as("ITIN_TRIP_SEQ_NUM_R"),
      col("TRIP_DEP_DT").as("TRIP_DEP_DT_R"),
      col("ITIN_PART_TYPE_CDE").as("ITIN_PART_TYPE_CDE_R"),
      col("TRIP_SEG_CT").as("TRIP_SEG_CT_R"),
      col("CONN_TYPE_CDE").as("CONN_TYPE_CDE_R"),
      col("PAX_CHK_IN_ID").as("PAX_CHK_IN_ID_R"),
      col("ACPT_CHNL_ORIG_CDE").as("ACPT_CHNL_ORIG_CDE_R"),
      col("ACPT_CHNL_TYPE_CDE").as("ACPT_CHNL_TYPE_CDE_R"),
      col("BRDG_PASS_GRP_CDE").as("BRDG_PASS_GRP_CDE_R"),
      col("BRDG_PASS_GRP_SEQ_NUM").as("BRDG_PASS_GRP_SEQ_NUM_R"),
      col("SEAT_NUM_ID").as("SEAT_NUM_ID_R"),
      col("PAX_BRD_DT").as("PAX_BRD_DT_R"),
      col("PAX_BRD_H2MI").as("PAX_BRD_H2MI_R"),
      col("CHK_BAG_STAT_CDE").as("CHK_BAG_STAT_CDE_R"),
      col("APIS_QCK_QRY_STAT_CDE").as("APIS_QCK_QRY_STAT_CDE_R"),
      col("BUS_SEL_FLAG").as("BUS_SEL_FLAG_R"),
      col("EARLY_BIRD_FLAG").as("EARLY_BIRD_FLAG_R"),
      col("DOM_INTL_CDE").as("DOM_INTL_CDE_R"),
      col("LATEST_JOB_ID").as("LATEST_JOB_ID_R"),
      col("CHK_BAG_CT").as("CHK_BAG_CT_R"),
      col("PNR_PAX_SEQ_NUM").as("PNR_PAX_SEQ_NUM_R"),
      col("SEG_DEP_DT").as("SEG_DEP_DT_R"),
      col("RES_SYS_PAX_ID").as("RES_SYS_PAX_ID_R"),
      col("ORIGINAL_JOB_ID").as("ORIGINAL_JOB_ID_R"),
      col("CDE_SHR_TYPE_CDE").as("CDE_SHR_TYPE_CDE_R"),
      col("SEG_ARR_UTC_OFST_H2MI").as("SEG_ARR_UTC_OFST_H2MI_R"),
      col("EFF_TO_CENT_TZ").as("EFF_TO_CENT_TZ_R"),
      col("OPNG_FLT_NUM").as("OPNG_FLT_NUM_R"),
      col("TRIP_SEG_SEQ_NUM").as("TRIP_SEG_SEQ_NUM_R"),
      col("MKTG_FLT_NUM").as("MKTG_FLT_NUM_R"),
      col("SEG_DEP_H2MI").as("SEG_DEP_H2MI_R"),
      col("OPNG_CARR_CDE").as("OPNG_CARR_CDE_R"),
      col("PAX_LAP_INF_FLAG").as("PAX_LAP_INF_FLAG_R"),
      col("NONREV_TYPE_CDE").as("NONREV_TYPE_CDE_R"),
      col("ITIN_SEG_SEQ_NUM").as("ITIN_SEG_SEQ_NUM_R"),
      col("SEG_DEST_ARPT_CDE").as("SEG_DEST_ARPT_CDE_R"),
      col("SEG_ORIG_ARPT_CDE").as("SEG_ORIG_ARPT_CDE_R"),
      col("OPNL_INFO_COPY_FLAG").as("OPNL_INFO_COPY_FLAG_R"),
      col("SEG_BKNG_DT").as("SEG_BKNG_DT_R"),
      col("SEG_ARR_H2MI").as("SEG_ARR_H2MI_R"),
      col("SEG_DEP_UTC_OFST_H2MI").as("SEG_DEP_UTC_OFST_H2MI_R"),
      col("PNR_ENVLP_NUM").as("PNR_ENVLP_NUM_R"),
      col("TBL_NME").as("TBL_NME_R"),
      col("CUR_TS").as("CUR_TS_R"),
      col("MIN_EFF_FM_FLAG").as("MIN_EFF_FM_FLAG_R"),
      col("MAX_EFF_TO_FLAG").as("MAX_EFF_TO_FLAG_R")
    )
    val out = dfGroupBy.agg(
      max(col("EFF_FM_CENT_TZ")).as("EFF_FM_CENT_TZ"),
      col("RES_SYS_ID").as("RES_SYS_ID"),
      col("PNR_REC_LOC_ID").as("PNR_REC_LOC_ID"),
      col("PNR_CRE_DT").as("PNR_CRE_DT"),
      col("SEG_ARR_DT").as("SEG_ARR_DT"),
      col("FARE_OTHR_TATO_NUM").as("FARE_OTHR_TATO_NUM"),
      col("FARE_BSIS_CDE").as("FARE_BSIS_CDE"),
      col("MKTG_CARR_CDE").as("MKTG_CARR_CDE"),
      col("SEG_BKNG_H2MI").as("SEG_BKNG_H2MI"),
      col("SEG_ACTN_CDE").as("SEG_ACTN_CDE"),
      col("ORGL_SEG_ACTN_CDE").as("ORGL_SEG_ACTN_CDE"),
      col("PAX_CT").as("PAX_CT"),
      col("EXTRA_SEAT_CT").as("EXTRA_SEAT_CT"),
      col("BKNG_CLSS_CDE").as("BKNG_CLSS_CDE"),
      col("TRIP_ID").as("TRIP_ID"),
      col("TRIP_ORIG_ARPT_CDE").as("TRIP_ORIG_ARPT_CDE"),
      col("TRIP_DEST_ARPT_CDE").as("TRIP_DEST_ARPT_CDE"),
      col("ITIN_TRIP_SEQ_NUM").as("ITIN_TRIP_SEQ_NUM"),
      col("TRIP_DEP_DT").as("TRIP_DEP_DT"),
      col("ITIN_PART_TYPE_CDE").as("ITIN_PART_TYPE_CDE"),
      col("TRIP_SEG_CT").as("TRIP_SEG_CT"),
      col("CONN_TYPE_CDE").as("CONN_TYPE_CDE"),
      col("PAX_CHK_IN_ID").as("PAX_CHK_IN_ID"),
      col("ACPT_CHNL_ORIG_CDE").as("ACPT_CHNL_ORIG_CDE"),
      col("ACPT_CHNL_TYPE_CDE").as("ACPT_CHNL_TYPE_CDE"),
      col("BRDG_PASS_GRP_CDE").as("BRDG_PASS_GRP_CDE"),
      col("BRDG_PASS_GRP_SEQ_NUM").as("BRDG_PASS_GRP_SEQ_NUM"),
      col("SEAT_NUM_ID").as("SEAT_NUM_ID"),
      col("PAX_BRD_DT").as("PAX_BRD_DT"),
      col("PAX_BRD_H2MI").as("PAX_BRD_H2MI"),
      col("CHK_BAG_STAT_CDE").as("CHK_BAG_STAT_CDE"),
      col("APIS_QCK_QRY_STAT_CDE").as("APIS_QCK_QRY_STAT_CDE"),
      col("BUS_SEL_FLAG").as("BUS_SEL_FLAG"),
      col("EARLY_BIRD_FLAG").as("EARLY_BIRD_FLAG"),
      col("DOM_INTL_CDE").as("DOM_INTL_CDE"),
      col("LATEST_JOB_ID").as("LATEST_JOB_ID"),
      col("CHK_BAG_CT").as("CHK_BAG_CT"),
      col("PNR_PAX_SEQ_NUM").as("PNR_PAX_SEQ_NUM"),
      col("SEG_DEP_DT").as("SEG_DEP_DT"),
      col("RES_SYS_PAX_ID").as("RES_SYS_PAX_ID"),
      col("ORIGINAL_JOB_ID").as("ORIGINAL_JOB_ID"),
      col("CDE_SHR_TYPE_CDE").as("CDE_SHR_TYPE_CDE"),
      col("SEG_ARR_UTC_OFST_H2MI").as("SEG_ARR_UTC_OFST_H2MI"),
      col("EFF_TO_CENT_TZ").as("EFF_TO_CENT_TZ"),
      col("OPNG_FLT_NUM").as("OPNG_FLT_NUM"),
      col("TRIP_SEG_SEQ_NUM").as("TRIP_SEG_SEQ_NUM"),
      col("MKTG_FLT_NUM").as("MKTG_FLT_NUM"),
      col("SEG_DEP_H2MI").as("SEG_DEP_H2MI"),
      col("OPNG_CARR_CDE").as("OPNG_CARR_CDE"),
      col("PAX_LAP_INF_FLAG").as("PAX_LAP_INF_FLAG"),
      col("NONREV_TYPE_CDE").as("NONREV_TYPE_CDE"),
      col("ITIN_SEG_SEQ_NUM").as("ITIN_SEG_SEQ_NUM"),
      col("SEG_DEST_ARPT_CDE").as("SEG_DEST_ARPT_CDE"),
      col("SEG_ORIG_ARPT_CDE").as("SEG_ORIG_ARPT_CDE"),
      col("OPNL_INFO_COPY_FLAG").as("OPNL_INFO_COPY_FLAG"),
      col("SEG_BKNG_DT").as("SEG_BKNG_DT"),
      col("SEG_ARR_H2MI").as("SEG_ARR_H2MI"),
      col("SEG_DEP_UTC_OFST_H2MI").as("SEG_DEP_UTC_OFST_H2MI"),
      col("PNR_ENVLP_NUM").as("PNR_ENVLP_NUM"),
      col("TBL_NME").as("TBL_NME"),
      col("CUR_TS").as("CUR_TS"),
      col("MIN_EFF_FM_FLAG").as("MIN_EFF_FM_FLAG"),
      col("MAX_EFF_TO_FLAG").as("MAX_EFF_TO_FLAG")
    )

    out.rdd.setName("Prophecy|Aggregate|")
    out
  }

  def targetStore(sparkSession: SparkSession,in:DataFrame):Unit={
    val schemaArg = StructType(
      Array(
        StructField("RES_SYS_ID",            StringType,    true),
        StructField("PNR_REC_LOC_ID",        StringType,    true),
        StructField("PNR_CRE_DT",            DateType,      true),
        StructField("SEG_ARR_DT",            DateType,      true),
        StructField("FARE_OTHR_TATO_NUM",    StringType,    true),
        StructField("FARE_BSIS_CDE",         StringType,    true),
        StructField("MKTG_CARR_CDE",         StringType,    true),
        StructField("SEG_BKNG_H2MI",         StringType,    true),
        StructField("SEG_ACTN_CDE",          StringType,    true),
        StructField("ORGL_SEG_ACTN_CDE",     StringType,    true),
        StructField("PAX_CT",                StringType,    true),
        StructField("EXTRA_SEAT_CT",         StringType,    true),
        StructField("BKNG_CLSS_CDE",         StringType,    true),
        StructField("TRIP_ID",               StringType,    true),
        StructField("TRIP_ORIG_ARPT_CDE",    StringType,    true),
        StructField("TRIP_DEST_ARPT_CDE",    StringType,    true),
        StructField("ITIN_TRIP_SEQ_NUM",     StringType,    true),
        StructField("TRIP_DEP_DT",           DateType,      true),
        StructField("ITIN_PART_TYPE_CDE",    StringType,    true),
        StructField("TRIP_SEG_CT",           StringType,    true),
        StructField("CONN_TYPE_CDE",         StringType,    true),
        StructField("PAX_CHK_IN_ID",         StringType,    true),
        StructField("ACPT_CHNL_ORIG_CDE",    StringType,    true),
        StructField("ACPT_CHNL_TYPE_CDE",    StringType,    true),
        StructField("BRDG_PASS_GRP_CDE",     StringType,    true),
        StructField("BRDG_PASS_GRP_SEQ_NUM", StringType,    true),
        StructField("SEAT_NUM_ID",           StringType,    true),
        StructField("PAX_BRD_DT",            DateType,      true),
        StructField("PAX_BRD_H2MI",          StringType,    true),
        StructField("CHK_BAG_STAT_CDE",      StringType,    true),
        StructField("APIS_QCK_QRY_STAT_CDE", StringType,    true),
        StructField("BUS_SEL_FLAG",          StringType,    true),
        StructField("EARLY_BIRD_FLAG",       StringType,    true),
        StructField("DOM_IntegerTypeL_CDE",  StringType,    true),
        StructField("LATEST_JOB_ID",         StringType,    true),
        StructField("CHK_BAG_CT",            StringType,    true),
        StructField("PNR_PAX_SEQ_NUM",       StringType,    true),
        StructField("SEG_DEP_DT",            DateType,      true),
        StructField("RES_SYS_PAX_ID",        StringType,    true),
        StructField("ORIGINAL_JOB_ID",       StringType,    true),
        StructField("CDE_SHR_TYPE_CDE",      StringType,    true),
        StructField("SEG_ARR_UTC_OFST_H2MI", StringType,    true),
        StructField("EFF_TO_CENT_TZ",        TimestampType, true),
        StructField("OPNG_FLT_NUM",          StringType,    true),
        StructField("TRIP_SEG_SEQ_NUM",      StringType,    true),
        StructField("MKTG_FLT_NUM",          StringType,    true),
        StructField("SEG_DEP_H2MI",          StringType,    true),
        StructField("EFF_FM_CENT_TZ",        TimestampType, true),
        StructField("OPNG_CARR_CDE",         StringType,    true),
        StructField("PAX_LAP_INF_FLAG",      StringType,    true),
        StructField("NONREV_TYPE_CDE",       StringType,    true),
        StructField("ITIN_SEG_SEQ_NUM",      StringType,    true),
        StructField("SEG_DEST_ARPT_CDE",     StringType,    true),
        StructField("SEG_ORIG_ARPT_CDE",     StringType,    true),
        StructField("OPNL_INFO_COPY_FLAG",   StringType,    true),
        StructField("SEG_BKNG_DT",           DateType,      true),
        StructField("SEG_ARR_H2MI",          StringType,    true),
        StructField("SEG_DEP_UTC_OFST_H2MI", StringType,    true),
        StructField("PNR_ENVLP_NUM",         LongType,      true),
        StructField("TBL_NME",               StringType,    true),
        StructField("CUR_TS",                StringType,    true),
        StructField("MIN_EFF_FM_FLAG",       IntegerType,   true),
        StructField("MAX_EFF_TO_FLAG",       IntegerType,   true)
      )
    )
    val keyColumns = List("SEG_ORIG_ARPT_CDE",
      "RES_SYS_ID",
      "PNR_REC_LOC_ID",
      "PNR_CRE_DT",
      "SEG_DEP_DT",
      "RES_SYS_PAX_ID",
      "OPNG_FLT_NUM",
      "OPNG_CARR_CDE",
      "PAX_LAP_INF_FLAG"
    )
    val scdHistoricColumns   = List("PNR_ENVLP_NUM")
    val fromTimeColumn       = "EFF_FM_CENT_TZ"
    val toTimeColumn         = "EFF_TO_CENT_TZ"
    val minFlagColumn        = "MIN_EFF_FM_FLAG"
    val maxFlagColumn        = "MAX_EFF_TO_FLAG"
    val exitingTableLocation = "dbfs:/FileStore/tables/Target_CDC/"
    val flagY                = 1
    val flagN                = 0

    val updateColumns: Array[String] = in.columns
    val updatesDF = in

    val existingTable: DeltaTable = DeltaTable.forPath("dbfs:/FileStore/tables/Target_CDC/")
    val existingDF:    DataFrame  = existingTable.toDF
    val rowsToUpdate = updatesDF
      .join(existingDF, keyColumns)
      .where(
        existingDF.col(s"$maxFlagColumn") === lit(flagY) && (
          scdHistoricColumns
            .map { scdCol =>
              existingDF.col(scdCol) =!= updatesDF.col(scdCol)
            }
            .reduce((c1, c2) => c1 || c2)
          )
      )
      .select(updateColumns.map(updatesDF.col): _*)
      .withColumn(s"$minFlagColumn", lit(flagN))
    val stagedUpdatesDF = rowsToUpdate
      .withColumn("mergeKey",                 lit(null))
      .union(updatesDF.withColumn("mergeKey", concat(keyColumns.map(col): _*)))
    existingTable
      .as("existingTable")
      .merge(
        stagedUpdatesDF.as("staged_updates"),
        concat(keyColumns.map(existingDF.col): _*) === stagedUpdatesDF("mergeKey")
      )
      .whenMatched(
        existingDF.col(s"$maxFlagColumn") === lit(flagY) && (
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
          s"$toTimeColumn" -> s"staged_updates.$fromTimeColumn"
        )
      )
      .whenNotMatched()
      .insertAll()
      .execute()
  }
}
