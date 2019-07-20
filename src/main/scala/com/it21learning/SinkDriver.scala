package com.it21learning

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.spark.sql.SparkSession
import com.it21learning.config.Settings
import com.it21learning.data._
import com.it21learning.process.consumers.{SinkConsumer, TrainConsumer}

object SinkDriver {
  //the main entry
  def main(args: Array[String]): Unit = {
    //initialize configuration
    Settings.initialize()

    //the spark-session
    val spark = SparkSession
      .builder
        .config("spark.cassandra.connection.host", Settings.Sink.host)
        .config("spark.cassandra.connection.port", Settings.Sink.port.toString())
      .getOrCreate()
    try {
      //users
      val sinkUsers = (new SinkConsumer(User, false) with Settings.Source.Consumer)
      //user-friends
      val sinkUserFriends = (new SinkConsumer(UserFriend, false) with Settings.Source.Consumer)
      //events
      val sinkEvents = (new SinkConsumer(Event, false) with Settings.Source.Consumer)
      //event-attendees
      val sinkEventAttendees = (new SinkConsumer(EventAttendee, false) with Settings.Source.Consumer)
      //train
      val sinkTrain = (new TrainConsumer(false) with Settings.Source.Consumer)
      //all consumers
      while ( true ) {
        //users
        val numUsers = sinkUsers.run(spark)
        println("The # of Users sinked - %d".format(numUsers))
        //user-friends
        val numUserFriends = sinkUserFriends.run(spark)
        println("The # of User-Friends sinked - %d".format(numUserFriends))
        //events
        val numEvents = sinkEvents.run(spark)
        println("The # of Events sinked - %d".format(numEvents))
        //event-attendees
        val numEventAttendees = sinkEventAttendees.run(spark)
        println("The # of Events-Attendees sinked - %d".format(numEventAttendees))
        //event-attendees
        val numTrain = sinkTrain.run(spark)
        println("The # of Train sinked - %d".format(numTrain))

        //check
        if ( (numUsers + numUserFriends + numEvents + numEventAttendees + numTrain) <= 0 ) {
          //sleep
          Thread.sleep(90000 )
        }
      }
    }
    finally {
      //stop the session
      spark.stop()
    }
  }
}
