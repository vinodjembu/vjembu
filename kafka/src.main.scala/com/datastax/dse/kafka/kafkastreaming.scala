package com.datastax.dse.kafka

import org.apache.spark.sql.types._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql._
import com.datastax.spark.connector.streaming._
import org.apache.spark.sql.streaming.OutputMode
/**
 * Spark Structure Streaming - Kafka
 * Writing data from Kafka to Cassandra
 * 
 * @author vinodjembu
 */
object kafkastreaming {

  def main(args: Array[String]) {
   
    val spark = SparkSession
      .builder
      .appName("Kafka Spark Streaming")
      .enableHiveSupport()
      .getOrCreate()

    val inputDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "dsetopic")
      .option("failOnDataLoss", "false")
      .load()

    print("Reading stream....\n")

    val personJsonDf = inputDf.selectExpr("CAST(value AS STRING)")

    val struct = new StructType()
      .add("firstname", DataTypes.StringType)
      .add("lastname", DataTypes.StringType)
      .add("birthdate", DataTypes.StringType)

    import org.apache.spark.sql.functions.from_json
    import org.apache.spark.sql.functions.col

    val personNestedDf = personJsonDf.select(from_json(col("value"), struct).as("person"))
    val personFlattenedDf = personNestedDf.selectExpr("person.firstname", "person.lastname", "person.birthdate")

    //Writing to Console
    /* val consoleOutput = personFlattenedDf.writeStream
      .option("checkpointLocation", "file:///Users/vinodjembu/Documents/loadFiles/pt2")
      .outputMode("append")
      .format("console")
      .start()

      consoleOutput.awaitTermination() */


    //Writing to Cassandra
    val output = personFlattenedDf.writeStream
      .option("checkpointLocation", "dsefs:///chkpt/")
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "bootcamp")
      .option("table", "kafkastream")
      .outputMode(OutputMode.Update).start()

    output.awaitTermination()

  }

}