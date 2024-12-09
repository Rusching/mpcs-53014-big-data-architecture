package org.example
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Increment, Put, Table}
import org.apache.hadoop.hbase.util.Bytes

object StreamTaxiTrips {

  // Jackson ObjectMapper for JSON deserialization
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  // HBase connection configuration
  val hbaseConf: Configuration = HBaseConfiguration.create()
  val hbaseConnection: Connection = ConnectionFactory.createConnection(hbaseConf)

  // Define HBase tables
  val taxiTripsTable: Table = hbaseConnection.getTable(TableName.valueOf("shichen_taxi_trips_speed"))

  // Kafka Taxi Trip Record Case Class
  case class KafkaTaxiTripRecord(
      tripId: String,
      taxiId: String,
      tripStartTimestamp: String,
      tripEndTimestamp: String,
      tripSeconds: Int,
      tripMiles: Double,
      pickupCensusTract: String,
      dropoffCensusTract: String,
      pickupCommunityArea: Int,
      dropoffCommunityArea: Int,
      fare: Double,
      tips: Double,
      tolls: Double,
      extras: Double,
      tripTotal: Double,
      paymentType: String,
      company: String,
      pickupCentroidLatitude: Double,
      pickupCentroidLongitude: Double,
      pickupCentroidLocation: String,
      dropoffCentroidLatitude: Double,
      dropoffCentroidLongitude: Double,
      dropoffCentroidLocation: String
  )

  // Function to insert/update data into HBase
  def updateHBaseWithTaxiTripRecord(record: KafkaTaxiTripRecord): String = {
    try {
      // HBase Put for storing data
      val put = new Put(Bytes.toBytes(record.tripId))
      put.addColumn(Bytes.toBytes("trip_info"), Bytes.toBytes("taxi_id"), Bytes.toBytes(record.taxiId))
      put.addColumn(Bytes.toBytes("trip_info"), Bytes.toBytes("trip_start_timestamp"), Bytes.toBytes(record.tripStartTimestamp))
      put.addColumn(Bytes.toBytes("trip_info"), Bytes.toBytes("trip_end_timestamp"), Bytes.toBytes(record.tripEndTimestamp))
      put.addColumn(Bytes.toBytes("trip_info"), Bytes.toBytes("trip_seconds"), Bytes.toBytes(record.tripSeconds))
      put.addColumn(Bytes.toBytes("trip_info"), Bytes.toBytes("trip_miles"), Bytes.toBytes(record.tripMiles))
      put.addColumn(Bytes.toBytes("trip_info"), Bytes.toBytes("pickup_census_tract"), Bytes.toBytes(record.pickupCensusTract))
      put.addColumn(Bytes.toBytes("trip_info"), Bytes.toBytes("dropoff_census_tract"), Bytes.toBytes(record.dropoffCensusTract))
      put.addColumn(Bytes.toBytes("trip_info"), Bytes.toBytes("pickup_community_area"), Bytes.toBytes(record.pickupCommunityArea))
      put.addColumn(Bytes.toBytes("trip_info"), Bytes.toBytes("dropoff_community_area"), Bytes.toBytes(record.dropoffCommunityArea))
      put.addColumn(Bytes.toBytes("trip_info"), Bytes.toBytes("fare"), Bytes.toBytes(record.fare))
      put.addColumn(Bytes.toBytes("trip_info"), Bytes.toBytes("tips"), Bytes.toBytes(record.tips))
      put.addColumn(Bytes.toBytes("trip_info"), Bytes.toBytes("tolls"), Bytes.toBytes(record.tolls))
      put.addColumn(Bytes.toBytes("trip_info"), Bytes.toBytes("extras"), Bytes.toBytes(record.extras))
      put.addColumn(Bytes.toBytes("trip_info"), Bytes.toBytes("trip_total"), Bytes.toBytes(record.tripTotal))
      put.addColumn(Bytes.toBytes("trip_info"), Bytes.toBytes("payment_type"), Bytes.toBytes(record.paymentType))
      put.addColumn(Bytes.toBytes("trip_info"), Bytes.toBytes("company"), Bytes.toBytes(record.company))
      put.addColumn(Bytes.toBytes("trip_info"), Bytes.toBytes("pickup_centroid_latitude"), Bytes.toBytes(record.pickupCentroidLatitude))
      put.addColumn(Bytes.toBytes("trip_info"), Bytes.toBytes("pickup_centroid_longitude"), Bytes.toBytes(record.pickupCentroidLongitude))
      put.addColumn(Bytes.toBytes("trip_info"), Bytes.toBytes("pickup_centroid_location"), Bytes.toBytes(record.pickupCentroidLocation))
      put.addColumn(Bytes.toBytes("trip_info"), Bytes.toBytes("dropoff_centroid_latitude"), Bytes.toBytes(record.dropoffCentroidLatitude))
      put.addColumn(Bytes.toBytes("trip_info"), Bytes.toBytes("dropoff_centroid_longitude"), Bytes.toBytes(record.dropoffCentroidLongitude))
      put.addColumn(Bytes.toBytes("trip_info"), Bytes.toBytes("dropoff_centroid_location"), Bytes.toBytes(record.dropoffCentroidLocation))

      // Write to HBase
      taxiTripsTable.put(put)
      s"Record for Trip ID ${record.tripId} successfully inserted into HBase"
    } catch {
      case e: Exception =>
        e.printStackTrace()
        s"Failed to insert record for Trip ID ${record.tripId}: ${e.getMessage}"
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: StreamTaxiTrips <brokers>
        |  <brokers> is a list of one or more Kafka brokers
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers) = args

    // Spark Streaming Configuration
    val sparkConf = new SparkConf().setAppName("StreamTaxiTrips")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Kafka topics and parameters
    val topicsSet = Set("shichen-taxi-trip-events")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "shichen_taxi_trip_stream_group",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    println(s"Creating Kafka stream with topics: ${topicsSet.mkString(",")} and group ID: ${kafkaParams("group.id")}")

    // Create Kafka Direct Stream
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )
    println("Kafka stream created successfully.")

    // Deserialize Kafka messages into KafkaTaxiTripRecord
    val serializedRecords = stream.map(_.value())

    serializedRecords.foreachRDD { rdd =>
      if (rdd.isEmpty()) {
        println("No data received in serializedRecords this batch.")
      } else {
        println("Received serializedRecords RDD")
        rdd.foreach(record => println(s"Serialized Record: $record"))
      }
    }

    val taxiTripRecords = serializedRecords.map(rec => mapper.readValue(rec, classOf[KafkaTaxiTripRecord]))

    taxiTripRecords.foreachRDD { rdd =>
      if (rdd.isEmpty()) {
        println("No data received in taxiTripRecords this batch.")
      } else {
        println("Received taxiTripRecords RDD")
        rdd.foreach(record => println(s"TaxiTripRecord: $record"))
      }
    }

    // Update HBase for each record

    val processedTrips = taxiTripRecords.map { record =>
      val result = updateHBaseWithTaxiTripRecord(record)
      println(s"Processing record: $record, Result: $result")
      result
    }

    processedTrips.foreachRDD { rdd =>
      rdd.foreach { result =>
        println(s"Processed result: $result")
      }
    }

    // Start the Streaming Context
    ssc.start()
    ssc.awaitTermination()
  }
}
