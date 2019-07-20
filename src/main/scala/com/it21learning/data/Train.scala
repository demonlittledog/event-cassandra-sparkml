package com.it21learning.data

import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import com.it21learning.common.Transferable

case class Train(user_id: String, event_id: String, invited: String, time_stamp: String, interested: String) {

}

object Train extends Transferable {
  //kafka topic
  def kafkaTopic: String = "train"
  //consumer group
  def consumerGroup: String = "grpTrain"

  //cassandra table
  def tableName: String = "train"

  //build
  def construct(rows: Seq[String], spark: SparkSession): DataFrame = {
    //create the objects
    val train: Seq[Train] = rows.map(s => s.split(",", -1))
      .map(f => Train(f(0), f(1), f(2), f(3), f(4)))
    //create
    import spark.implicits._
    spark.createDataset(train).toDF
  }
}