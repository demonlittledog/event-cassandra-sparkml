package com.it21learning.common

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract trait Transferable {
  //kafka topic
  def kafkaTopic: String
  //consumer group
  def consumerGroup: String

  //cassandra keyspace
  def keySpace: String = "events_db"
  //cassandra table
  def tableName: String

  //construct the data-frame
  def construct(rows: Seq[String], spark: SparkSession): DataFrame

  //build
  def transfer(rows: Seq[String], spark: SparkSession): Unit = {
    //construct
    construct(rows, spark).withColumn("counter_value", lit(1))
      .write
        .format("org.apache.spark.sql.cassandra")
        .option("table", tableName).option("keyspace", keySpace)
      .mode("append")
      .save()
  }
}

