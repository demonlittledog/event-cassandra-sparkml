package com.it21learning.data

import org.apache.spark.sql.{SparkSession, DataFrame}
import com.it21learning.common.Transferable

case class EventAttendee(event_id: String, user_id: String, attend_type: String) {

}

object EventAttendee extends Transferable {
  //kafka topic
  def kafkaTopic: String = "event_attendees"
  //consumer group
  def consumerGroup: String = "grpEventAttendees"

  //cassandra table
  def tableName: String = "event_attendee"

  //build
  def construct(rows: Seq[String], spark: SparkSession): DataFrame = {
    //create the objects
    val eventAttendees: Seq[EventAttendee] = rows.map(s => s.split(",", -1))
      .map(f => EventAttendee(f(0), f(1), f(2)))
    //create
    import spark.implicits._
    spark.createDataset(eventAttendees).toDF
  }
}