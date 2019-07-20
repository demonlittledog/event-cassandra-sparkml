package com.it21learning.data

import org.apache.spark.sql.{SparkSession, DataFrame}
import com.it21learning.common.Transferable

case class Event(event_id: String, start_time: String, city: String, state: String, zip: String, country: String, latitude: String, longitude: String, user_id: String, common_words: String) {

}

object Event extends Transferable {
  //kafka topic
  def kafkaTopic: String = "events"
  //consumer group
  def consumerGroup: String = "grpEvents"

  //cassandra table
  def tableName: String = "events"

  //build
  def construct(rows: Seq[String], spark: SparkSession): DataFrame = {
    //organize the common-words
    val cws: StringBuilder = new StringBuilder()
    //get common words
    def getCommonWords(fields: Array[String]): String = {
      //clear
      cws.clear()
      //check
      if ( fields.length > 8 ) {
        //loop through
        for (i: Int <- 9 until fields.length) {
          //check
          if ( cws.length > 0 ) {
            //the separator
            cws.append("|")
          }
          //append
          cws.append(fields(i))
        }
      }
      //result
      cws.toString()
    }

    //create the objects
    val events: Seq[Event] = rows.map(s => s.split(",", -1))
      .map(f => Event(f(0), f(2), f(3), f(4), f(5), f(6), f(7), f(8), f(1), getCommonWords(f)))
    //create
    import spark.implicits._
    spark.createDataset(events).toDF
  }
}