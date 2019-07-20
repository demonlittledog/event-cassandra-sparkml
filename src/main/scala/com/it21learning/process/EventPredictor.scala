package com.it21learning.process

import com.it21learning.sink.SinkStatement
import com.it21learning.config.settings.{DatabaseSettings, DebugSettings}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.util.{Date, Properties}
import java.text.SimpleDateFormat

/* -----------------------------------------------------------------------------------------------------------

 * The following code is created by Wayne Shen for the Big Data Training
 *
 * Date: Sept. 29, 2018
 * Copyright @ iT21 Learning
 *
 ---------------------------------------------------------------------------------------------------------- */
@SerialVersionUID(20180722L)
class EventPredictor(spark: SparkSession, debug: Option[DebugSettings] = None)
      extends EventTransformer(spark) with Serializable { self: DatabaseSettings =>
  //properties for writing to mysql
  private val props = new Properties()
  //driver
  props.put("driver", this.sinkDbDriver)
  //user
  props.put("user", this.sinkDBUser)
  //password
  props.put("password", this.sinkDBPwd)

  //process
  def process(rddTest: RDD[String]): Unit = {
    //only need the value column
    import EventPredictor.{testColumns => columns}
    import spark.implicits._
    val dfTest = rddTest.map(r => r.split(",", -1)).toDF("value")
      .select((0 until columns.length).map(i => col("value").getItem(i).as(columns(i))): _*)
    //save to mysql
    dfTest.select(
        col("user_id"),
        col("event_id"),
        col("invited").cast(IntegerType).as("invited"),
        col("time_stamp"))
      .write.mode("append").jdbc(this.sinkDbUrl, "test", this.props)
    //check
    debug match {
      case Some(debugSettings) => debugSettings.write(dfTest, "test")
      case _ => { /* do nothing */ }
    }

    //events data-frame
    val dfEvents = dfTest.drop("user_id").drop("invited").drop("time_stamp")
      .withColumnRenamed("event_creator", "user_id")
    //save to mysql
    dfEvents.select(
        col("event_id"),
        col("start_time"),
        col("city"),
        col("state"),
        col("zip"),
        col("country"),
        col("latitude").cast(FloatType).as("latitude"),
        col("longitude").cast(FloatType).as("longitude"),
        col("user_id"))
      .write.mode("append").jdbc(this.sinkDbUrl, "events", this.props)
    //check
    debug match {
      case Some(debugSettings) => debugSettings.write(dfEvents, "events")
      case _ => { /* do nothing */ }
    }

    //user-event
    val dfUserEvent = getUserEvent(dfTest)
    //check
    debug match {
      case Some(debugSettings) => debugSettings.write(dfUserEvent, "user_event")
      case _ => { /* do nothing */ }
    }
    //event-creator-is-friend
    val dfEventCreatorIsFriend = getEventCreatorIsFriend(dfUserEvent, "user_friend", "user_friend_count")
    //check
    debug match {
      case Some(debugSettings) => debugSettings.write(dfEventCreatorIsFriend, "event_creator_is_friend")
      case _ => { /* do nothing */ }
    }
    //friend attend percentage
    val dfFriendAttendPercentage = getFriendAttendPercentage(dfEventCreatorIsFriend, "friend_attend_summary")
    //check
    debug match {
      case Some(debugSettings) => debugSettings.write(dfFriendAttendPercentage, "friend_attend_percentage")
      case _ => { /* do nothing */ }
    }
    //user event count
    val dfUserEventCount = getUserEventCount(dfUserEvent)
    //check
    debug match {
      case Some(debugSettings) => debugSettings.write(dfUserEventCount, "user_event_count")
      case _ => { /* do nothing */ }
    }
    //save to mysql
    dfUserEventCount.select(col("user_id"), col("event_count"))
      .write.mode("overwrite").jdbc(this.sinkDbUrl, "user_event_count", this.props)

    //user-friend-event
    val dfUserFriendEVent = getUserFriendEvent(dfFriendAttendPercentage, dfUserEventCount, "users",
      "event_attendee_count", "user_attend_event_count", "event_cities", "event_countries", "locale")
    //check
    debug match {
      case Some(debugSettings) => debugSettings.write(dfUserFriendEVent, "user_friend_event")
      case _ => { /* do nothing */ }
    }

    //feature set
    val dfFeatureSet = prepareFeatureSet(dfUserFriendEVent)
    //check
    debug match {
      case Some(debugSettings) => debugSettings.write(dfFeatureSet, "feature_set")
      case _ => { /* do nothing */ }
    }
    //load the model
    val model = ModelLoader().get()
    //the current time-stamp
    val tms = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date())
    //predict
    val dfPredictions = model.transform(dfFeatureSet.select(dfFeatureSet.columns.map(c => col(c).cast(DoubleType)): _*))
      .drop(col("user_interested"))
      .select(
        col("user_id").cast(LongType).cast(StringType).as("user_id"),
        col("event_id").cast(LongType).cast(StringType).as("event_id"),
        col("prediction").cast(IntegerType).as("user_interested"),
        lit(tms).as("prediction_time"))
    //check
    debug match {
      case Some(debugSettings) => debugSettings.write(dfPredictions, "event_predictions")
      case _ => { /* do nothing */ }
    }

    //write to mysql
    val props = new Properties()
    //driver
    props.put("driver", this.sinkDbDriver)
    //user
    props.put("user", this.sinkDBUser)
    //password
    props.put("password", this.sinkDBPwd)
    //save
    dfPredictions.write.mode("append").jdbc(this.sinkDbUrl, "predictions", props)
  }

  //prepare user event
  private def getUserEvent(dfTest: DataFrame): DataFrame = {
    //calculate
    dfTest.withColumn("start_facts", startFacts(col("start_time")))
      .select(
        col("user_id"),
        col("event_id"),
        col("invited").as("user_invited"),
        daysFromInviteTime(col("time_stamp")).as("invite_ahead_days"),
        lit(0).as("user_interested"),
        col("event_creator"),
        daysFromStartTime(col("start_time")).as("start_ahead_days"),
        col("start_facts").getItem(0).as("event_start_month"),
        col("start_facts").getItem(1).as("event_start_dayofweek"),
        col("start_facts").getItem(2).as("event_start_hour"),
        col("city").as("event_city"),
        col("state").as("event_state"),
        col("country").as("event_country"),
        col("latitude"),
        col("longitude"))
      .drop(col("start_facts"))
  }

  //get the user event count
  private def getUserEventCount(dfUserEvent: DataFrame): DataFrame = {
    //register
    dfUserEvent.createOrReplaceTempView("user_event")
    //user event count
    spark.read
      .format("org.apache.spark.sql.cassandra")
      .option("table", "user_event_count").option("keyspace", "events_db")
      .load()
      .createOrReplaceTempView("user_event_count")
    //union
    val stmt = """SELECT
          user_id,
          count(event_id) AS event_count
        FROM user_event
        GROUP BY user_id
      union
        SELECT
          user_id,
          event_count
        FROM user_event_count"""
    //execute
    spark.sql(stmt)
  }
}

//the companion object
object EventPredictor {
  //the columns of the test data-set
  def testColumns = "user_id,event_id,invited,time_stamp,event_creator,start_time,city,state,zip,country,latitude,longitude,c_1,c_2,c_3,c_4,c_5,c_6,c_7,c_8,c_9,c_10,c_11,c_12,c_13,c_14,c_15,c_16,c_17,c_18,c_19,c_20,c_21,c_22,c_23,c_24,c_25,c_26,c_27,c_28,c_29,c_30,c_31,c_32,c_33,c_34,c_35,c_36,c_37,c_38,c_39,c_40,c_41,c_42,c_43,c_44,c_45,c_46,c_47,c_48,c_49,c_50,c_51,c_52,c_53,c_54,c_55,c_56,c_57,c_58,c_59,c_60,c_61,c_62,c_63,c_64,c_65,c_66,c_67,c_68,c_69,c_70,c_71,c_72,c_73,c_74,c_75,c_76,c_77,c_78,c_79,c_80,c_81,c_82,c_83,c_84,c_85,c_86,c_87,c_88,c_89,c_90,c_91,c_92,c_93,c_94,c_95,c_96,c_97,c_98,c_99,c_100,c_other".split(",")

  //the statement for inserting predictions
  trait PredictionStatement extends SinkStatement {
    //make a statement based on the Row
    def make(value: Row): String = "INSERT INTO predictions(user_id, event_id, interested) VALUES('%s', '%s', %d);".format(value.getString(0), value.getString(1), value.getInt(2))
  }
}
