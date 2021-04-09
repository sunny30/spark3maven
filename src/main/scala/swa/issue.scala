//package swa
//
//import io.delta.tables.DeltaTable
//import org.apache.spark.sql.types._
//import io.prophecy.libs._
//import io.prophecy.libs.UDFUtils._
//import io.prophecy.libs.Component._
//import io.prophecy.libs.DataHelpers._
//import io.prophecy.libs.SparkFunctions._
//import io.prophecy.libs.UDFUtils._
//import io.prophecy.libs.FixedFileFormatImplicits._
//import org.apache.spark.sql.ProphecyDataFrame._
//import org.apache.spark._
//import org.apache.spark.sql._
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types._
//
//type SetOperation = DataFrame
//
//object ConfigStore {
//  case class Config(fabricName: String)
//}
//object ConfigStoreValues {
//  implicit var Config: ConfigStore.Config = ConfigStore.Config(fabricName = "default")
//}
//import ConfigStoreValues._
//
//object DedupPaxAir {
//
//  def apply(spark: SparkSession, in: DataFrame): DedupSorted = {
//    import spark.implicits._
//
//
//    //Code for DedupSorted DedupPaxAir
//    val out = in.dedupSort(
//      typeToKeep = "first",
//      groupByColumns = List(col("reservationSystemCustomerID"), col("passengerTypeCode"), col("marketingCarrierCode"), col("marketingFlightNumber"), col("departureDateTime"), col("departureAirportCode"))
//    )
//
//    //End of Code for DedupSorted DedupPaxAir
//
//
//
//    out
//
//  }
//
//
//
//}
//
//
//object FlattenPaxAir {
//
//  def apply(spark: SparkSession, in: DataFrame): FlattenSchema = {
//    import spark.implicits._
//
//
//    //Code for FlattenSchema FlattenPaxAir
//
//    val shortenNames = true
//    val delimiter = "_"
//
//    val flattened = in.withColumn("CdsFeed_feedBody_Reservation_boundSegmentList_boundSegment", explode(col("CdsFeed.feedBody.Reservation.boundSegmentList.boundSegment"))).withColumn("CdsFeed_feedBody_Reservation_boundSegmentList_boundSegment_boundDetails", explode(col("CdsFeed_feedBody_Reservation_boundSegmentList_boundSegment.boundDetails"))).withColumn("CdsFeed_feedBody_Reservation_airFareList_airFare_airFarePricingList_airFarePricing", explode(col("CdsFeed.feedBody.Reservation.airFareList.airFare.airFarePricingList.airFarePricing"))).withColumn("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem", explode(col("CdsFeed.feedBody.Reservation.passengerList.passenger.reservationItemList.reservationItem")))
//    val out = flattened.select(col("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem_arrivalInfo_arrivalDate").as("arrivalDate"), col("CdsFeed.feedBody.Reservation.airFareList.airFare.airFareSequenceID").as("airFareSequenceID"), col("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem_arrivalInfo_arrivalDateUtc").as("arrivalDateUtc"), col("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem.departureInfo.departureDateTimeUtc").as("departureDateTimeUtc"), col("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem.bookingCreationInfo.bookingCreationDate").as("bookingCreationDate"), col("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem.departureInfo.departureAirportCode").as("departureAirportCode"), col("CdsFeed.feedBody.Reservation.passengerList.passenger.reservationSystemCustomerID").as("reservationSystemCustomerID"), col("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem.marketingCarrierCode").as("marketingCarrierCode"), col("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem.bookingCreationInfo.bookingCreationTimeZoneCode").as("bookingCreationTimeZoneCode"), col("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem.segmentStatusCode").as("segmentStatusCode"), col("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem.originalSegmentStatusCode").as("originalSegmentStatusCode"), col("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem.passengerCountOnSegment").as("passengerCountOnSegment"), col("CdsFeed_feedBody_Reservation_pnrGroupInfo_groupUnassignedQuantity").as("groupUnassignedQuantity"), col("CdsFeed.feedBody.Reservation.airFareList.airFare.passengerTypeCode").as("passengerTypeCode"), col("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem.classOfServiceCode").as("classOfServiceCode"), col("CdsFeed_feedBody_Reservation_boundSegmentList_boundSegment_boundDetails.extendedRoutingKey").as("extendedRoutingKey"), col("CdsFeed_feedBody_Reservation_boundSegmentList_boundSegment_boundDetails.originAirportCode").as("originAirportCode"), col("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem.departureInfo.departureDateTime").as("departureDateTime"), col("CdsFeed_feedBody_Reservation_boundSegmentList_boundSegment_boundDetails.direction").as("direction"), col("CdsFeed_feedBody_Reservation_boundSegmentList_boundSegment_boundDetails.boundItemSequenceNumber").as("boundItemSequenceNumber"), col("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem_reservationServiceLineElementsList_reservationServiceLineElements_reservationServiceLineElementID").as("reservationServiceLineElementID"), col("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem.reservationItemID").as("reservationItemID"), col("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem_paxActivityStatusList_paxActivityStatus_paxActivityDetails_subType").as("subType"), col("CdsFeed_feedBody_Reservation_passengerList_passenger_ssrList_serviceRequest_specialServiceRequestTypeCode").as("specialServiceRequestTypeCode"), col("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem_segmentFlownIndicator").as("segmentFlownIndicator"), col("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem_reservationServiceLineElementsList_reservationServiceLineElements_freeText").as("freeText"), col("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem.marketingFlightNumber").as("marketingFlightNumber"), col("CdsFeed.feedBody.Reservation.pnrCreateDate").as("pnrCreateDate"), col("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem_overbookingDetail_overbookingTypeCode").as("overbookingTypeCode"), col("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem.arrivalInfo.arrivalAirportCode").as("arrivalAirportCode"), col("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem_overbookingDetail_overbookingReasonCode").as("overbookingReasonCode"), col("CdsFeed_feedBody_Reservation_airFareList_airFare_airFarePricingList_airFarePricing.fareBasisPrimaryCode").as("fareBasisPrimaryCode"), col("CdsFeed.feedBody.Reservation.reservationSystemCode").as("reservationSystemCode"), col("CdsFeed.feedBody.Reservation.lastUpdateTimestamp").as("lastUpdateTimestamp"), col("CdsFeed.feedBody.Reservation.pnrReservationSystemSequenceID").as("pnrReservationSystemSequenceID"), col("CdsFeed_feedBody_ReservationList_Reservation_passengerList_passenger_reservationItemList_reservationItem_reservationSystemPassengerReservationItemID").as("reservationSystemPassengerReservationItemID"), col("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem_departureInfo_departureDateUtc").as("departureDateUtc"), col("CdsFeed.feedBody.Reservation.recordLocatorID").as("recordLocatorID"))
//    //End of Code for FlattenSchema FlattenPaxAir
//
//
//
//    out
//
//  }
//
//
//
//}
//
//
//object TableWisePaxAir {
//
//  def apply(spark: SparkSession, in: DataFrame): SchemaTransformer = {
//    import spark.implicits._
//
//
//    //Code for SchemaTransformer TableWisePaxAir
//    val out = in.withColumn("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem_arrivalInfo_arrivalDate", lit(null).cast(StringType)).withColumn("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem_arrivalInfo_arrivalDateUtc", lit(null).cast(StringType)).withColumn("CdsFeed_feedBody_Reservation_pnrGroupInfo_groupUnassignedQuantity", lit(null).cast(StringType)).withColumn("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem_reservationServiceLineElementsList_reservationServiceLineElements_reservationServiceLineElementID", lit(null).cast(StringType)).withColumn("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem_paxActivityStatusList_paxActivityStatus_paxActivityDetails_subType", lit(null).cast(StringType)).withColumn("CdsFeed_feedBody_Reservation_passengerList_passenger_ssrList_serviceRequest_specialServiceRequestTypeCode", lit(null).cast(StringType)).withColumn("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem_segmentFlownIndicator", lit(null).cast(StringType)).withColumn("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem_reservationServiceLineElementsList_reservationServiceLineElements_freeText", lit(null).cast(StringType)).withColumn("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem_overbookingDetail_overbookingTypeCode", lit(null).cast(StringType)).withColumn("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem_overbookingDetail_overbookingReasonCode", lit(null).cast(StringType)).withColumn("CdsFeed_feedBody_ReservationList_Reservation_passengerList_passenger_reservationItemList_reservationItem_reservationSystemPassengerReservationItemID", lit(null).cast(StringType)).withColumn("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem_departureInfo_departureDateUtc", lit(null).cast(StringType))  //End of Code for SchemaTransformer TableWisePaxAir
//
//
//
//    out
//
//  }
//
//
//
//}
//
//
//object FilterPaxAir {
//
//  def apply(spark: SparkSession, in: DataFrame): Filter = {
//    import spark.implicits._
//
//
//    //Code for Filter FilterPaxAir
//    val out = in.filter(col("reservationSystemCustomerID").isNotNull && col("passengerTypeCode").isNotNull && col("marketingCarrierCode").isNotNull && col("marketingFlightNumber").isNotNull && col("departureDateTime").isNotNull && col("departureAirportCode").isNotNull)
//    //End of Code for Filter FilterPaxAir
//
//
//
//    out
//
//  }
//
//
//
//}
//
//
//object swa_ingest_target_scd2 {
//
//  @UsesDataset(id = "156", version = 1)
//  def apply(spark: SparkSession, in: DataFrame): Target = {
//    import spark.implicits._
//
//
//    //Code for Target swa_ingest_target_scd2
//    val fabric = "default"
//    fabric match {
//      case "default" =>
//        val schemaArg = StructType(Array(StructField("RES_SYS_ID", StringType, true), StructField("PNR_REC_LOC_ID", StringType, true), StructField("PNR_CRE_DT", DateType, true), StructField("SEG_ARR_DT", DateType, true), StructField("FARE_OTHR_TATO_NUM", LongType, true), StructField("FARE_BSIS_CDE", StringType, true), StructField("MKTG_CARR_CDE", StringType, true), StructField("SEG_BKNG_H2MI", StringType, true), StructField("SEG_ACTN_CDE", StringType, true), StructField("ORGL_SEG_ACTN_CDE", StringType, true), StructField("PAX_CT", LongType, true), StructField("TAKN_SEAT_CT", StringType, true), StructField("EXTRA_SEAT_CT", StringType, true), StructField("BKNG_CLSS_CDE", StringType, true), StructField("TRIP_ID", StringType, true), StructField("TRIP_ORIG_ARPT_CDE", StringType, true), StructField("TRIP_DEST_ARPT_CDE", StringType, true), StructField("ITIN_TRIP_SEQ_NUM", StringType, true), StructField("TRIP_DEP_DT", DateType, true), StructField("ITIN_PART_TYPE_CDE", StringType, true), StructField("TRIP_SEG_CT", LongType, true), StructField("TRIP_DCS_LEG_CT", StringType, true), StructField("SEG_DCS_LEG_CT", StringType, true), StructField("CONN_TYPE_CDE", LongType, true), StructField("STBY_LISTED_FLAG", StringType, true), StructField("PAX_CHK_IN_ID", StringType, true), StructField("ACPT_STAT_CDE", StringType, true), StructField("ACPT_CHNL_ORIG_CDE", StringType, true), StructField("ACPT_CHNL_TYPE_CDE", StringType, true), StructField("BRDG_PASS_GRP_CDE", StringType, true), StructField("BRDG_PASS_GRP_SEQ_NUM", StringType, true), StructField("SEAT_NUM_ID", StringType, true), StructField("PAX_BRD_STAT_CDE", StringType, true), StructField("PAX_BRD_DT", DateType, true), StructField("PAX_BRD_H2MI", StringType, true), StructField("CHK_BAG_STAT_CDE", StringType, true), StructField("PAX_REC_STAT_CDE", StringType, true), StructField("APIS_QCK_QRY_STAT_CDE", StringType, true), StructField("BUS_SEL_FLAG", StringType, true), StructField("EARLY_BIRD_FLAG", StringType, true), StructField("FLWN_FLAG", StringType, true), StructField("DOM_INTL_CDE", StringType, true), StructField("LATEST_JOB_ID", StringType, true), StructField("CHK_BAG_CT", StringType, true), StructField("OVBK_REAS_CDE", StringType, true), StructField("PNR_PAX_SEQ_NUM", StringType, true), StructField("SEG_DEP_DT", DateType, true), StructField("RES_SYS_PAX_ID", StringType, true), StructField("ORIGINAL_JOB_ID", StringType, true), StructField("CDE_SHR_TYPE_CDE", StringType, true), StructField("SEG_ARR_UTC_OFST_H2MI", StringType, true), StructField("EFF_TO_CENT_TZ", TimestampType, true), StructField("OPNG_FLT_NUM", LongType, true), StructField("TRIP_SEG_SEQ_NUM", LongType, true), StructField("MKTG_FLT_NUM", LongType, true), StructField("SEG_DEP_H2MI", StringType, true), StructField("EFF_FM_CENT_TZ", TimestampType, true), StructField("OPNG_CARR_CDE", StringType, true), StructField("PAX_LAP_INF_FLAG", StringType, true), StructField("NONREV_TYPE_CDE", StringType, true), StructField("OVBK_TYPE_CDE", StringType, true), StructField("RES_SYS_PAX_SEG_ID", StringType, true), StructField("ITIN_SEG_SEQ_NUM", StringType, true), StructField("SEG_DEST_ARPT_CDE", StringType, true), StructField("SEG_ORIG_ARPT_CDE", StringType, true), StructField("ACPT_CANC_REAS_CDE", StringType, true), StructField("OPNL_INFO_COPY_FLAG", StringType, true), StructField("SEG_BKNG_DT", DateType, true), StructField("SEG_ARR_H2MI", StringType, true), StructField("SEG_DEP_UTC_OFST_H2MI", StringType, true), StructField("PNR_ENVLP_NUM", LongType, true), StructField("MAX_EFF_TO_FLAG", IntegerType, true), StructField("CUR_TBL", StringType, true), StructField("CUR_TS", StringType, true), StructField("min_eff_fm_flag", IntegerType, true)))
//        val keyColumns = List("SEG_ORIG_ARPT_CDE","RES_SYS_ID","PNR_REC_LOC_ID","PNR_CRE_DT","SEG_DEP_DT","RES_SYS_PAX_ID","OPNG_FLT_NUM","OPNG_CARR_CDE","PAX_LAP_INF_FLAG")
//        val scdHistoricColumns = List("PNR_ENVLP_NUM")
//        val fromTimeColumn     = "EFF_FM_CENT_TZ"
//        val toTimeColumn       = "EFF_TO_CENT_TZ"
//        val minFlagColumn      =  "MIN_EFF_FM_FLAG"
//        val maxFlagColumn      =  "MAX_EFF_TO_FLAG"
//        val exitingTableLocation = "dbfs:/FileStore/tables/swa/ingest_target/"
//        val flagY              = 1
//        val flagN              = 0
//
//        val updateColumns: Array[String] = in.columns
//        in.write.format("delta").save("dbfs:/FileStore/tables/swa/ingest_target/")
//
//      //     val updatesDF = in
//
//      //     val existingTable: DeltaTable = DeltaTable.forPath("dbfs:/FileStore/tables/swa/ingest_target/")
//      //     val existingDF: DataFrame = existingTable.toDF
//      //    val rowsToUpdate = updatesDF.join(existingDF, keyColumns).where(existingDF.col(s"$maxFlagColumn") === lit(flagY) &&  (
//      //           scdHistoricColumns.map {
//      //             scdCol => existingDF.col(scdCol) =!= updatesDF.col(scdCol)
//      //           }.reduce ( (c1, c2) => c1 || c2 )
//      //           ))
//      //    .select(updateColumns.map(updatesDF.col): _*).withColumn(s"$minFlagColumn" ,lit(flagN))
//      //    val stagedUpdatesDF = rowsToUpdate.withColumn("mergeKey", lit(null)).union( updatesDF.withColumn("mergeKey", concat(keyColumns.map(col): _*)))
//      // existingTable
//      //       .as("existingTable")
//      //       .merge(
//      //         stagedUpdatesDF.as("staged_updates"),
//      //         concat(keyColumns.map(existingDF.col): _*) === stagedUpdatesDF("mergeKey")
//      //        )
//      //       .whenMatched (
//      //           existingDF.col(s"$maxFlagColumn") === lit(flagY) && (
//      //           scdHistoricColumns.map {
//      //             scdCol => existingDF.col(scdCol) =!= stagedUpdatesDF.col(scdCol)
//      //           }.reduce ( (c1, c2) => c1 || c2 )
//      //          )
//      //        )
//      //       .updateExpr(
//      //          Map(
//      //             s"$maxFlagColumn" -> s"$flagN",
//      //             s"$toTimeColumn" -> s"staged_updates.$fromTimeColumn"
//      //          )
//      //       )
//      //       .whenNotMatched()
//      //       .insertAll()
//      //       .execute()   case _ => throw new Exception("Unknown Fabric")}  //End of Code for Target swa_ingest_target_scd2
//
//
//
//
//
//    }
//
//
//
//  }
//
//
//  object swa_ingest_source {
//
//    @UsesDataset(id = "155", version = 0)
//    def apply(spark: SparkSession): Source = {
//      import spark.implicits._
//
//
//      //Code for Source swa_ingest_source
//      val fabric = "default"
//
//      lazy val out =  fabric match {
//        case "default" =>
//          val schemaArg = StructType(Array(StructField("CdsFeed", StructType(Array(StructField("FeedDomain", StringType, true), StructField("FeedVersion", DoubleType, true), StructField("feedBody", StructType(Array(StructField("Reservation", StructType(Array(StructField("agentInfo", StructType(Array(StructField("creatorAgent", StructType(Array(StructField("agentAirportCode", StringType, true), StructField("agentCountryCode", StringType, true), StructField("agentDeliverySystemID", StringType, true), StructField("agentIATACode", LongType, true), StructField("agentTypeCode", StringType, true), StructField("employeeID", StringType, true), StructField("sosSystemDescription", StringType, true), StructField("sosSystemLocation", StringType, true), StructField("sosSystemOfficeCode", StringType, true), StructField("swaOfficeID", StringType, true))), true), StructField("ownerAgent", StructType(Array(StructField("agentAirportCode", StringType, true), StructField("agentCountryCode", StringType, true), StructField("agentDeliverySystemID", StringType, true), StructField("agentIATACode", LongType, true), StructField("agentLocationCode", StringType, true), StructField("agentTypeCode", StringType, true), StructField("sosSystemDescription", StringType, true), StructField("sosSystemLocation", StringType, true), StructField("sosSystemOfficeCode", StringType, true), StructField("swaOfficeID", StringType, true))), true), StructField("queueingAgent", StructType(Array(StructField("queueingAgentOfficeID", StringType, true))), true), StructField("updatorAgent", StructType(Array(StructField("agentAirportCode", StringType, true), StructField("agentCountryCode", StringType, true), StructField("agentDeliverySystemID", StringType, true), StructField("agentDutyID", StringType, true), StructField("agentIATACode", LongType, true), StructField("agentSignID", StringType, true), StructField("agentTypeCode", StringType, true), StructField("employeeID", StringType, true), StructField("receivedFrom", StringType, true), StructField("sosSystemDescription", StringType, true), StructField("sosSystemLocation", StringType, true), StructField("sosSystemOfficeCode", StringType, true), StructField("swaOfficeID", StringType, true), StructField("workstationID", StringType, true))), true))), true), StructField("airFareList", StructType(Array(StructField("airFare", StructType(Array(StructField("agentOfficeID", StringType, true), StructField("agentSignID", StringType, true), StructField("airFareInfoList", StructType(Array(StructField("airFareInfo", ArrayType(StructType(Array(StructField("fareAmount", DoubleType, true), StructField("fareCurrencyCode", StringType, true), StructField("fareTypeCategory", StringType, true), StructField("fareTypeCode", StringType, true))), true), true))), true), StructField("airFarePricingList", StructType(Array(StructField("airFarePricing", ArrayType(StructType(Array(StructField("connectionIndicatorCode", StringType, true), StructField("fareBasisPrimaryCode", StringType, true), StructField("lastUpdateTimestamp", StringType, true), StructField("reservationItemID", LongType, true))), true), true))), true), StructField("airFareRemarkList", StructType(Array(StructField("transitionalRemark", StructType(Array(StructField("cmn:lastUpdateTimeStamp", StringType, true), StructField("cmn:remarkText", StringType, true), StructField("cmn:remarkTypeCode", StringType, true))), true))), true), StructField("airFareSequenceID", LongType, true), StructField("airFareTaxList", StructType(Array(StructField("airFareTax", ArrayType(StructType(Array(StructField("cmn:currencyCode", StringType, true), StructField("cmn:lastUpdateTimestamp", StringType, true), StructField("cmn:natureCode", StringType, true), StructField("cmn:taxAmount", DoubleType, true), StructField("cmn:taxCategoryCode", StringType, true), StructField("cmn:taxCountryCode", StringType, true))), true), true))), true), StructField("createDateTime", StringType, true), StructField("fareCalculationMode", StringType, true), StructField("involIndicator", BooleanType, true), StructField("issueCode", StringType, true), StructField("lastTicketingDateTime", StringType, true), StructField("lastUpdateTimestamp", StringType, true), StructField("manualPriceIndicator", StringType, true), StructField("originCityCode", StringType, true), StructField("passengerID", LongType, true), StructField("passengerQuantity", LongType, true), StructField("passengerTypeCode", StringType, true), StructField("pricingDateTime", StringType, true))), true))), true), StructField("boundSegmentList", StructType(Array(StructField("boundSegment", ArrayType(StructType(Array(StructField("boundDetails", ArrayType(StructType(Array(StructField("accessControlCode", LongType, true), StructField("actualIndicator", BooleanType, true), StructField("arrivalAirportCode", StringType, true), StructField("arrivalCityCode", StringType, true), StructField("arrivalCountryCode", StringType, true), StructField("boundCategory", StringType, true), StructField("boundGroup", LongType, true), StructField("boundItemSequenceNumber", LongType, true), StructField("boundLevel", StringType, true), StructField("boundType", StringType, true), StructField("direction", StringType, true), StructField("extendedRoutingKey", StringType, true), StructField("hostAirlineIndicator", BooleanType, true), StructField("intendedIndicator", BooleanType, true), StructField("originAirportCode", StringType, true), StructField("originCityCode", StringType, true), StructField("originCountryCode", StringType, true), StructField("redundantIndicator", BooleanType, true), StructField("routingKey", StringType, true), StructField("span", StringType, true))), true), true), StructField("reservationSystemSegmentID", LongType, true))), true), true))), true), StructField("contactList", StructType(Array(StructField("contact", StructType(Array(StructField("contactInformation", StringType, true), StructField("contactMediumTypeCode", LongType, true), StructField("contactTypeID", LongType, true), StructField("lastUpdateTimestamp", StringType, true))), true))), true), StructField("lastUpdateTimestamp", StringType, true), StructField("passengerCount", LongType, true), StructField("passengerList", StructType(Array(StructField("passenger", StructType(Array(StructField("contactList", StructType(Array(StructField("contact", ArrayType(StructType(Array(StructField("contactInformation", StringType, true), StructField("contactMediumTypeCode", LongType, true), StructField("contactPreferenceMediumTypeCode", StringType, true), StructField("contactPreferenceType", StringType, true), StructField("contactTypeID", LongType, true), StructField("lastUpdateTimestamp", StringType, true))), true), true))), true), StructField("dateOfBirth", StringType, true), StructField("farePassengerTypeCode", StringType, true), StructField("gender", StringType, true), StructField("givenName", StringType, true), StructField("groupDepositIndicator", BooleanType, true), StructField("infantIndicator", BooleanType, true), StructField("lastUpdateTimestamp", StringType, true), StructField("passengerFOPList", StructType(Array(StructField("passengerFOP", ArrayType(StructType(Array(StructField("fopFreeText", StringType, true), StructField("fopID", LongType, true), StructField("fopSupplementaryInfoList", StructType(Array(StructField("fopSupplementaryInfo", ArrayType(StructType(Array(StructField("type", StringType, true), StructField("value", StringType, true))), true), true))), true), StructField("lastUpdateTimestamp", StringType, true))), true), true))), true), StructField("passengerID", LongType, true), StructField("passengerLoyaltyList", StructType(Array(StructField("passengerLoyalty", ArrayType(StructType(Array(StructField("carrierCode", StringType, true), StructField("customerValue", LongType, true), StructField("lastUpdateTimestamp", StringType, true), StructField("passengerLoyaltyID", LongType, true), StructField("passengerLoyaltyTypeCode", StringType, true), StructField("priorityCode", LongType, true), StructField("tierLevel", StringType, true))), true), true))), true), StructField("passengerTicketDocList", StructType(Array(StructField("passengerTicketDoc", ArrayType(StructType(Array(StructField("chargeAmount", DoubleType, true), StructField("currency", StringType, true), StructField("issuanceDate", StringType, true), StructField("lastUpdateTimestamp", StringType, true), StructField("status", StringType, true), StructField("ticketDocID", LongType, true), StructField("ticketIndicator", StringType, true), StructField("ticketNumber", LongType, true), StructField("ticketType", StringType, true))), true), true))), true), StructField("passengerTravelDocList", StructType(Array(StructField("passengerTravelDoc", StructType(Array(StructField("dateOfBirth", StringType, true), StructField("gender", StringType, true), StructField("givenName", StringType, true), StructField("surName", StringType, true))), true))), true), StructField("passengerTypeCode", StringType, true), StructField("postalAddressList", StructType(Array(StructField("postalAddress", StructType(Array(StructField("addressLine1", StringType, true), StructField("addressTypeCode", StringType, true), StructField("addressTypeID", LongType, true), StructField("city", StringType, true), StructField("contactMediumTypeCode", StringType, true), StructField("country", StringType, true), StructField("lastUpdateTimestamp", StringType, true), StructField("postalCode", LongType, true), StructField("state", StringType, true))), true))), true), StructField("reservationItemList", StructType(Array(StructField("reservationItem", ArrayType(StructType(Array(StructField("arrivalInfo", StructType(Array(StructField("arrivalAirportCode", StringType, true), StructField("arrivalDateTime", StringType, true), StructField("arrivalDateTimeUtc", StringType, true))), true), StructField("bookingCreationInfo", StructType(Array(StructField("bookingCreationDate", StringType, true), StructField("bookingCreationTimeZoneCode", StringType, true))), true), StructField("classOfServiceCode", StringType, true), StructField("dayChangeIndicator", LongType, true), StructField("departureInfo", StructType(Array(StructField("departureAirportCode", StringType, true), StructField("departureDateTime", StringType, true), StructField("departureDateTimeUtc", StringType, true))), true), StructField("iataEquipmentTypeCode", StringType, true), StructField("lastUpdateTimeStamp", StringType, true), StructField("marketingCarrierCode", StringType, true), StructField("marketingFlightNumber", LongType, true), StructField("originalSegmentStatusCode", StringType, true), StructField("passengerCountOnSegment", LongType, true), StructField("pnrRelatedReservation", StructType(Array(StructField("recordLocator", StringType, true), StructField("relation", StringType, true), StructField("reservationSystemCode", StringType, true))), true), StructField("pointofSaleInfo", StructType(Array(StructField("pointOfSaleAgentCityCode", StringType, true), StructField("pointOfSaleAgentCountryCode", StringType, true), StructField("pointOfSaleAgentOfficeID", StringType, true), StructField("pointOfSaleAgentTypeCode", StringType, true), StructField("pointOfSaleCorpCode", StringType, true), StructField("pointOfSaleReservationSystemSignID", LongType, true))), true), StructField("remarkList", StructType(Array(StructField("remark", StructType(Array(StructField("cmn:lastUpdateTimeStamp", StringType, true), StructField("cmn:remarkText", StringType, true), StructField("cmn:remarkTypeCode", StringType, true), StructField("remarkID", LongType, true))), true))), true), StructField("reservationItemID", LongType, true), StructField("reservationItemSequence", LongType, true), StructField("reservationSystemPassengerReservationItemID", StringType, true), StructField("reservationSystemSegmentID", LongType, true), StructField("segmentStatusCode", StringType, true), StructField("segmentType", StringType, true), StructField("ssrList", StructType(Array(StructField("serviceRequest", ArrayType(StructType(Array(StructField("chargeableIndicator", BooleanType, true), StructField("companyID", StringType, true), StructField("freeText", StringType, true), StructField("lastUpdateTimeStamp", StringType, true), StructField("requestQuantity", LongType, true), StructField("specialServiceRequestCategoryCode", StringType, true), StructField("specialServiceRequestTypeCode", StringType, true), StructField("specialServiceRequestTypeID", LongType, true), StructField("statusCode", StringType, true))), true), true))), true))), true), true))), true), StructField("reservationSystemCustomerID", StringType, true), StructField("seatCount", LongType, true), StructField("ssrList", StructType(Array(StructField("serviceRequest", StructType(Array(StructField("chargeableIndicator", BooleanType, true), StructField("companyID", StringType, true), StructField("freeText", StringType, true), StructField("lastUpdateTimeStamp", StringType, true), StructField("requestQuantity", LongType, true), StructField("specialServiceRequestCategoryCode", StringType, true), StructField("specialServiceRequestTypeCode", StringType, true), StructField("specialServiceRequestTypeID", LongType, true), StructField("statusCode", StringType, true))), true))), true), StructField("surName", StringType, true))), true))), true), StructField("pnrCreateDate", StringType, true), StructField("pnrPurgeDate", StringType, true), StructField("pnrReservationSystemSequenceID", LongType, true), StructField("productPriceList", StructType(Array(StructField("productPrice", ArrayType(StructType(Array(StructField("agentOfficeID", StringType, true), StructField("associatedDocumentNumber", LongType, true), StructField("createDateTime", StringType, true), StructField("internationalIndicator", StringType, true), StructField("involIndicator", BooleanType, true), StructField("issueTypeCode", StringType, true), StructField("lastUpdateTimestamp", StringType, true), StructField("productPriceFareList", StructType(Array(StructField("productPriceFare", ArrayType(StructType(Array(StructField("fareAmount", LongType, true), StructField("fareCurrencyCode", StringType, true), StructField("fareTypeCategory", StringType, true), StructField("fareTypeCode", StringType, true))), true), true))), true), StructField("productPriceRemarkList", StructType(Array(StructField("transitionalRemark", StructType(Array(StructField("cmn:lastUpdateTimeStamp", StringType, true), StructField("cmn:remarkText", StringType, true), StructField("cmn:remarkTypeCode", StringType, true))), true))), true), StructField("productPriceSequenceID", LongType, true), StructField("productPricingCouponList", StructType(Array(StructField("productPricingCoupon", ArrayType(StructType(Array(StructField("feeOwnerCode", StringType, true), StructField("lastUpdateTimestamp", StringType, true), StructField("reasonForIssuanceSubcode", StringType, true), StructField("reasonForIssuanceSubcodeDescription", StringType, true))), true), true))), true), StructField("productTypeCode", StringType, true))), true), true))), true), StructField("recordLocatorID", StringType, true), StructField("reservationSystemCode", StringType, true), StructField("xmlns", StringType, true), StructField("xmlns:cmn", StringType, true), StructField("xmlns:xs", StringType, true))), true))), true), StructField("feedHeader", StructType(Array(StructField("deploymentActivity", StringType, true), StructField("feedCreationDateTime", StructType(Array(StructField("content", LongType, true), StructField("iso-datetime", StringType, true))), true), StructField("feedCreationHost", StringType, true), StructField("feedCreationProcessId", StringType, true), StructField("feedCreationSystem", StringType, true), StructField("feedDomain", StringType, true), StructField("feedDomainVersion", DoubleType, true), StructField("feedId", StringType, true), StructField("feedMajorVersion", LongType, true), StructField("feedVersion", DoubleType, true), StructField("processingComparedVersion", LongType, true), StructField("rawId", StringType, true), StructField("sourceInfo", StructType(Array(StructField("originatingSystemDateTime", StringType, true), StructField("originatingSystemIdentifier", StringType, true), StructField("orignatingSystem", StructType(Array(StructField("company", StringType, true), StructField("domain", StringType, true))), true))), true), StructField("sourceSystemVersion", LongType, true), StructField("versionProcessingCounter", LongType, true))), true), StructField("xmlns", StringType, true), StructField("xmlns:xsi", StringType, true), StructField("xsi:schemaLocation", StringType, true))), true)))
//          spark.read.format("json")
//            .option("multiLine", true)
//            .schema(schemaArg)
//            .load("dbfs:/FileStore/tables/swa_pax_air_main.json").cache()
//        case _ => throw new Exception(s"The fabric '$fabric' is not handled")}  //End of Code for Source swa_ingest_source
//
//
//
//      out
//
//    }
//
//
//
//  }
//
//
//  object ReformatPaxAir {
//
//    def apply(spark: SparkSession, in: DataFrame): Reformat = {
//      import spark.implicits._
//
//
//      //Code for Reformat ReformatPaxAir
//      val out = in.select(
//        coalesce(col("reservationSystemCode"), lit("-")).as("RES_SYS_ID"),
//        coalesce(col("recordLocatorID"), lit("-")).as("PNR_REC_LOC_ID"),
//        substring(col("pnrCreateDate"), 1, 10).cast(DateType).as("PNR_CRE_DT"),
//        substring(col("arrivalDate"), 1, 10).cast(DateType).as("SEG_ARR_DT"),
//        coalesce(col("airFareSequenceID"), lit("-")).as("FARE_OTHR_TATO_NUM"),
//        coalesce(col("fareBasisPrimaryCode"), lit("-")).as("FARE_BSIS_CDE"),
//        coalesce(col("marketingCarrierCode"), lit("-")).as("MKTG_CARR_CDE"),
//        coalesce(col("bookingCreationTimeZoneCode"), lit("-")).as("SEG_BKNG_H2MI"),
//        coalesce(col("segmentStatusCode"), lit("-")).as("SEG_ACTN_CDE"),
//        coalesce(col("originalSegmentStatusCode"), lit("-")).as("ORGL_SEG_ACTN_CDE"),
//        coalesce(col("passengerCountOnSegment"), lit("-")).as("PAX_CT"),
//        coalesce(col("groupUnassignedQuantity"), lit("-")).as("TAKN_SEAT_CT"),
//        coalesce(col("passengerTypeCode"), lit("-")).as("EXTRA_SEAT_CT"),
//        coalesce(col("classofServiceCode"), lit("-")).as("BKNG_CLSS_CDE"),
//        coalesce(col("extendedRoutingKey"), lit("-")).as("TRIP_ID"),
//        coalesce(col("originAirportCode"), lit("-")).as("TRIP_ORIG_ARPT_CDE"),
//        coalesce(col("arrivalAirportCode"), lit("-")).as("TRIP_DEST_ARPT_CDE"),
//        coalesce(col("extendedRoutingKey"), lit("-")).as("ITIN_TRIP_SEQ_NUM"),
//        substring(col("departureDateTime"), 1, 10).cast(DateType).as("TRIP_DEP_DT"),
//        coalesce(col("direction"), lit("-")).as("ITIN_PART_TYPE_CDE"),
//        coalesce(col("boundItemSequenceNumber"), lit("-")).as("TRIP_SEG_CT"),
//        coalesce(col("reservationServiceLineElementID"), lit("-")).as("TRIP_DCS_LEG_CT"),
//        coalesce(col("reservationServiceLineElementID"), lit("-")).as("SEG_DCS_LEG_CT"),
//        coalesce(col("reservationItemID"), lit("-")).as("CONN_TYPE_CDE"),
//        coalesce(col("subType"), lit("-")).as("STBY_LISTED_FLAG"),
//        coalesce(col("freeText"), lit("-")).as("PAX_CHK_IN_ID"),
//        coalesce(col("subType"), lit("-")).as("ACPT_STAT_CDE"),
//        coalesce(col("freeText"), lit("-")).as("ACPT_CHNL_ORIG_CDE"),
//        coalesce(col("freeText"), lit("-")).as("ACPT_CHNL_TYPE_CDE"),
//        coalesce(col("freeText"), lit("-")).as("BRDG_PASS_GRP_CDE"),
//        coalesce(col("freeText"), lit("-")).as("BRDG_PASS_GRP_SEQ_NUM"),
//        coalesce(col("freeText"), lit("-")).as("SEAT_NUM_ID"),
//        coalesce(col("subType"), lit("-")).as("PAX_BRD_STAT_CDE"),
//        substring(col("freeText"), 1, 10).cast(DateType).as("PAX_BRD_DT"),
//        coalesce(col("freeText"), lit("-")).as("PAX_BRD_H2MI"),
//        coalesce(col("freeText"), lit("-")).as("CHK_BAG_STAT_CDE"),
//        coalesce(col("subType"), lit("-")).as("PAX_REC_STAT_CDE"),
//        coalesce(col("freeText"), lit("-")).as("APIS_QCK_QRY_STAT_CDE"),
//        coalesce(col("fareBasisPrimaryCode"), lit("-")).as("BUS_SEL_FLAG"),
//        coalesce(col("specialServiceRequestTypeCode"), lit("-")).as("EARLY_BIRD_FLAG"),
//        coalesce(col("segmentFlownIndicator"), lit("-")).as("FLWN_FLAG"),
//        lit("1").as("DOM_INTL_CDE"),
//        lit("1").as("LATEST_JOB_ID"),
//        coalesce(col("freeText"), lit("-")).as("CHK_BAG_CT"),
//        coalesce(col("overbookingReasonCode"), lit("-")).as("OVBK_REAS_CDE"),
//        lit('-').as("PNR_PAX_SEQ_NUM"),
//        substring(col("departureDateTime"), 1, 10).cast(DateType).as("SEG_DEP_DT"),
//        coalesce(col("reservationSystemCustomerID"), lit("-")).as("RES_SYS_PAX_ID"),
//        lit('1').as("ORIGINAL_JOB_ID"),
//        coalesce(col("marketingCarrierCode"), lit("-")).as("CDE_SHR_TYPE_CDE"),
//        coalesce(col("arrivalDateUtc"), lit("-")).as("SEG_ARR_UTC_OFST_H2MI"),
//        lit("2099-12-31 00:00:00").cast(TimestampType).as("EFF_TO_CENT_TZ"),
//        coalesce(col("marketingFlightNumber"), lit("-")).as("OPNG_FLT_NUM"),
//        coalesce(col("boundItemSequenceNumber"), lit("-")).as("TRIP_SEG_SEQ_NUM"),
//        coalesce(col("marketingFlightNumber"), lit("-")).as("MKTG_FLT_NUM"),
//        coalesce(col("departureDateTime"), lit("-")).as("SEG_DEP_H2MI"),
//        regexp_replace(col("lastUpdateTimestamp"), "T", " ").cast(TimestampType).as("EFF_FM_CENT_TZ"),
//        coalesce(col("marketingCarrierCode"), lit("-")).as("OPNG_CARR_CDE"),
//        coalesce(col("passengerTypeCode"), lit("-")).as("PAX_LAP_INF_FLAG"),
//        coalesce(col("fareBasisPrimaryCode"), lit("-")).as("NONREV_TYPE_CDE"),
//        coalesce(col("overbookingTypeCode"), lit("-")).as("OVBK_TYPE_CDE"),
//        coalesce(col("reservationSystemPassengerReservationItemID"), lit("-")).as("RES_SYS_PAX_SEG_ID"),
//        coalesce(col("departureDateTime"), lit("-")).as("ITIN_SEG_SEQ_NUM"),
//        lit("1").as("SEG_DEST_ARPT_CDE"),
//        coalesce(col("departureAirportCode"), lit("-")).as("SEG_ORIG_ARPT_CDE"),
//        coalesce(col("subType"), lit("-")).as("ACPT_CANC_REAS_CDE"),
//        lit("1").as("OPNL_INFO_COPY_FLAG"),
//        substring(col("bookingCreationDate"), 1, 10).cast(DateType).as("SEG_BKNG_DT"),
//        coalesce(col("arrivalDate"), lit("-")).as("SEG_ARR_H2MI"),
//        coalesce(col("departureDateUtc"), lit("-")).as("SEG_DEP_UTC_OFST_H2MI"),
//        col("pnrReservationSystemSequenceID").as("PNR_ENVLP_NUM"),
//        lit("RES_PAX_AIR").as("TBL_NME"),
//        lit("2020-07-06 14:47:09").as("CUR_TS"),
//        lit(1).as("MIN_EFF_FM_FLAG"),
//        lit(1).as("MAX_EFF_TO_FLAG")
//      )
//      //End of Code for Reformat ReformatPaxAir
//
//
//
//      out
//
//    }
//
//
//
//  }
//
//
//
//  spark.experimental.extraStrategies = InterimStrategy(spark) :: Nil
//  spark.conf.set("prophecy.execution.id", "7537de5a-ec5d")
//  implicit val session: SparkSession = spark
//
//
//  InterimStore.reset()
//
//  implicit val interimOutputConsole: InterimOutput = InterimOutputHive2("7537de5a-ec5d-4b57-9224-c965c0129290", "73", "3c88807c8307aaa8ea4da839e27f481f53ac375e", "7537de5a-ec5d-4b57-9224-c965c0129290")
//
//  //Code for Source swa_ingest_source
//  val df_swa_ingest_source: Source = swa_ingest_source(spark).interim("graph", "swa_ingest_source", "out", "swa_ingest_source_out", 40)
//  //Code for SubGraph res_pax_air_scd2
//  def res_pax_air_scd2(spark: SparkSession, in: DataFrame): SubGraph = {
//
//    //Code for SchemaTransformer TableWisePaxAir
//    val df_TableWisePaxAir: SchemaTransformer = TableWisePaxAir(spark, in).interim("res_pax_air_scd2", "TableWisePaxAir", "out", "TableWisePaxAir_out", 40)
//    //Code for FlattenSchema FlattenPaxAir
//    val df_FlattenPaxAir: FlattenSchema = FlattenPaxAir(spark, df_TableWisePaxAir).interim("res_pax_air_scd2", "FlattenPaxAir", "out", "FlattenPaxAir_out", 40)
//    //Code for DedupSorted DedupPaxAir
//    val df_DedupPaxAir: DedupSorted = DedupPaxAir(spark, df_FlattenPaxAir).interim("res_pax_air_scd2", "DedupPaxAir", "out", "DedupPaxAir_out", 40)
//    //Code for Filter FilterPaxAir
//    val df_FilterPaxAir: Filter = FilterPaxAir(spark, df_DedupPaxAir).interim("res_pax_air_scd2", "FilterPaxAir", "out", "FilterPaxAir_out", 40)
//    //Code for Reformat ReformatPaxAir
//    val out: Reformat = ReformatPaxAir(spark, df_FilterPaxAir).interim("res_pax_air_scd2", "ReformatPaxAir", "out", "ReformatPaxAir_out", 40)
//
//    // dangling data frames. inserting trigger action for these
//    // out.cache().count()
//
//    //    out
//
//    // }
//    // val df_res_pax_air_scd2: SubGraph = res_pax_air_scd2(spark, df_swa_ingest_source).interim("graph", "res_pax_air_scd2", "out", "res_pax_air_scd2_out", 40)
//    //Code for Target swa_ingest_target_scd2
//    swa_ingest_target_scd2(spark, df_res_pax_air_scd2)
//
//
//
