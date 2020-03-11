package com.datastax.dse.kafka

import org.apache.spark.sql.types._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql._
import com.datastax.spark.connector.streaming._
import org.apache.spark.sql.streaming.OutputMode
/**
 * Spark Structure Streaming - Kafka
 * Writing CSV data from Kafka to Cassandra
 *
 * @author vinodjembu
 */
object SparkStreamingcsv {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("Kafka Spark Streaming")
      .enableHiveSupport()
      .getOrCreate()

    val streamSchema = new StructType().add("entry_date", "date").add("buck_line_nbr", "integer").add("value1", "integer").add("value2", "integer").add("value3", "integer")
    
    //Loading CSV files from DSEFS
    val readdf = spark.readStream.option("header", "true").schema(streamSchema).csv("dsefs:///loadFiles/testing")

    //Writing to Console
    /* val consoleOutput = personFlattenedDf.writeStream
      .option("checkpointLocation", "file:///Users/vinodjembu/Documents/loadFiles/pt2")
      .outputMode("append")
      .format("console")
      .start()

      consoleOutput.awaitTermination() */

    //Writing to Cassandra
    val output = readdf.writeStream
      .option("checkpointLocation", "dsefs:///chkpt/")
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "bootcamp")
      .option("table", "streamtesting")
      .outputMode(OutputMode.Update).start()

    output.awaitTermination()

  }

}