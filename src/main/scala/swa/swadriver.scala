package swa

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object swadriver {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("databricks-sample1")
      .config("spark.default.parallelism", 4)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .getOrCreate()

    val nestedDF = spark.read.option("multiline", "true").option("inferSchema", "true").json("/Users/sharadsingh/Documents/samplemavensparkproject/swa.json")

    val semiFlatDF = nestedDF.withColumn("CdsFeed_feedBody_Reservation_boundSegmentList_boundSegment",explode(col("CdsFeed.feedBody.Reservation.boundSegmentList.boundSegment"))).
      withColumn("CdsFeed_feedBody_Reservation_boundSegmentList_boundSegment_boundDetails",explode(col("CdsFeed_feedBody_Reservation_boundSegmentList_boundSegment.boundDetails"))).
      withColumn("CdsFeed_feedBody_Reservation_airFareList_airFare_airFarePricingList_airFarePricing",explode(col("CdsFeed.feedBody.Reservation.airFareList.airFare.airFarePricingList.airFarePricing"))).
      withColumn("CdsFeed_feedBody_Reservation_passengerList_passenger_reservationItemList_reservationItem",explode(col("CdsFeed.feedBody.Reservation.passengerList.passenger.reservationItemList.reservationItem")))


  }

}
