package com.it21learning.process

import java.util.Properties

import com.it21learning.config.settings.{DatabaseSettings, DebugSettings, TrainSettings}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions.{lit, col}
import org.apache.spark.sql.types.{IntegerType, DoubleType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ModelTrainer(spark: SparkSession)
  extends EventTransformer(spark) with Serializable { self: TrainSettings with DatabaseSettings =>
  //debug flag
  private val debug: Option[DebugSettings] = if (this.debugEnabled) Some(this) else None

  //properties for writing to mysql
  private val props = new Properties()
  //driver
  props.put("driver", this.sinkDbDriver)
  //user
  props.put("user", this.sinkDBUser)
  //password
  props.put("password", this.sinkDBPwd)

  //start the training
  def train(): Unit = {
    //user-friend
    val dfUserFriend = getUserFriend()
      .filter(col("user_id").isNotNull && col("friend_id").isNotNull).cache()
    //transfer
    this.transfer(dfUserFriend, "user_friend")

    //user-friend count
    val dfUserFriendCount = getUserFriendCount(dfUserFriend).filter(col("user_id").isNotNull)
    //transfer
    this.transfer(dfUserFriendCount, "user_friend_count", Some("user_friend_count", "events_db"))

    //event-attendee
    val dfEventAttendee = getEventAttendee()
      .filter(col("event_id").isNotNull && col("user_id").isNotNull && col("attend_type").isNotNull).cache()
    //transfer
    this.transfer(dfEventAttendee, "event_attendee")

    //event-attendee count
    val (dfEventAttendeeCount, dfUserAttendStatus) = getEventAttendeeCountStatus(dfEventAttendee)
    //transfer
    this.transfer(dfEventAttendeeCount.filter(col("event_id").isNotNull && col("attend_type").isNotNull), "event_attendee_count", Some(("event_attendee_count", "events_db")))

    //user attend event count
    val dfUserAttendEventCount = getUserAttendEventCount(dfUserAttendStatus)
      .filter(col("user_id").isNotNull && col("user_id") =!= lit(""))
    //transfer
    this.transfer(dfUserAttendEventCount, "user_attend_event_count", Some(("user_attend_event_count", "events_db")))

    //friend attend summary
    val dfFriendAttendSummary = getFriendAttendSummary(dfUserFriend, dfUserAttendStatus)
      .filter(col("user_id").isNotNull && col("event_id").isNotNull)
    //transfer
    this.transfer(dfFriendAttendSummary, "friend_attend_summary", Some(("friend_attend_summary", "events_db")))

    //users
    val dfUsers = getUsers().filter(col("user_id").isNotNull)
    //transfer
    this.transfer(dfUsers, "users")

    //locale
    val dfLocale = getLocale().filter(col("locale_id").isNotNull)
    //transfer
    this.transfer(dfLocale, "locale", Some(("locale", "events_db")))

    //load train
    val dfTrain = getTrain()
    //transfer
    this.transfer(dfTrain, "train")

    //user event count
    val dfUserEventCount = getUserEventCount(dfTrain).filter(col("user_id").isNotNull)
    //transfer
    this.transfer(dfUserEventCount, "user_event_count", Some(("user_event_count", "events_db")))

    //load events
    val dfEvents = getEvents().filter(col("event_id").isNotNull)
    //transfer
    this.transfer(dfEvents, "events")

    //event cities & countries
    val (dfCities, dfCountries) = getEventCityCountry(dfEvents)
    //remove null values
    val dfEventCities = dfCities.filter(col("city").isNotNull && col("city") =!= lit(""))
    val dfEventCountries = dfCountries.filter(col("country").isNotNull && col("country") =!= lit(""))
    //transfer
    this.transfer(dfEventCities, "event_cities", Some(("event_cities", "events_db")))
    this.transfer(dfEventCountries, "event_countries", Some(("event_countries", "events_db")))

    //user event
    val dfUserEvent = getUserEvent(dfTrain, dfEvents)
    //event creator is a friend
    dfUserFriendCount.createOrReplaceTempView("user_friend_count")
    val dfEventCreatorIsFriend = getEventCreatorIsFriend(dfUserEvent, "user_friend", "user_friend_count")
    //friend attend percentage
    dfFriendAttendSummary.createOrReplaceTempView("friend_attend_summary")
    val dfFriendAttendPercentage = getFriendAttendPercentage(dfEventCreatorIsFriend, "friend_attend_summary")

    //user friend event
    dfUsers.createOrReplaceTempView("users")
    dfEventAttendeeCount.createOrReplaceTempView("event_attend_count")
    dfUserAttendEventCount.createOrReplaceTempView("user_attend_event_count")
    dfEventCities.createOrReplaceTempView("event_cities")
    dfEventCountries.createOrReplaceTempView("event_countries")
    dfLocale.createOrReplaceTempView("locale")
    val dfUserFriendEvent = getUserFriendEvent(dfFriendAttendPercentage, dfUserEventCount,
      "users", "event_attendee_count", "user_attend_event_count", "event_cities", "event_countries", "locale")
    //prepare feature-set
    val dfFeatureSet = prepareFeatureSet(dfUserFriendEvent)

    //build the model & save
    buildModel(dfFeatureSet).write.overwrite.save(this.modelDir)
  }

  //transfer
  private def transfer(df: DataFrame, odsTable: String, sink: Option[(String, String)] = None): Unit = {
    //save to bi-ods
    df.write.mode("overwrite").jdbc(this.sinkDbUrl, odsTable, props)

    //check
    debug match {
      case Some(debugSettings) => debugSettings.write(df, odsTable)
      case _ => { /* do nothing */ }
    }

    //check
    sink match {
      case Some((sinkTable, sinkKeyspace)) => df.withColumn("counter_value", lit(1))
        .write.format("org.apache.spark.sql.cassandra")
          .option("table", sinkTable).option("keyspace", sinkKeyspace).mode("append")
        .save()
      case _ => { /* do nothing */ }
    }
  }

  //load user-friend
  private def getUserFriend(): DataFrame = {
    //the user-friend
    spark.read.format("org.apache.spark.sql.cassandra").option("table", "user_friend").option("keyspace", "events_db").load()
  }
  //user-friend count
  private def getUserFriendCount(dfUserFriend: DataFrame): DataFrame = {
    //register
    dfUserFriend.createOrReplaceTempView("user_friend")

    //sql statement
    val stmt = """SELECT
        user_id,
        count(*) AS friend_count
      FROM user_friend WHERE friend_id IS NOT NULL AND friend_id != ''
      GROUP BY user_id"""
    //execute
    spark.sql(stmt)
  }

  //load event-attendee
  private def getEventAttendee(): DataFrame = {
    //the user-friend
    spark.read.format("org.apache.spark.sql.cassandra").option("table", "event_attendee").option("keyspace", "events_db").load()
  }

  //event-attendee count
  private def getEventAttendeeCountStatus(dfEventAttendee: DataFrame): (DataFrame, DataFrame) = {
    //register
    dfEventAttendee.createOrReplaceTempView("event_attendee")
    //sql statement
    val stmtCount = """SELECT
        event_id,
        attend_type,
        COUNT(user_id) AS attend_count
      FROM event_attendee
      GROUP BY event_id, attend_type"""
    //statement for status
    val stmtStatus = """SELECT
        t.event_id,
        t.attend_user_id,
        MAX(t.invited) AS invited,
        MAX(t.attended) as attended,
        MAX(t.not_attended) AS not_attended,
        MAX(t.maybe_attended) AS maybe_attended
      FROM (SELECT
          event_id,
          user_id AS attend_user_id,
          CASE WHEN attend_type = 'invited' THEN 1 ELSE 0 END AS invited,
          CASE WHEN attend_type = 'yes' THEN 1 ELSE 0 END AS attended,
          CASE WHEN attend_type = 'no' THEN 1 ELSE 0 END AS not_attended,
          CASE WHEN attend_type = 'maybe' THEN 1 ELSE 0 END AS maybe_attended
        FROM event_attendee) t
        GROUP BY event_id, attend_user_id"""
    //execute
    (spark.sql(stmtCount), spark.sql(stmtStatus))
  }

  //user-attend event-count
  private def getUserAttendEventCount(dfUserAttendStatus: DataFrame): DataFrame = {
    //register
    dfUserAttendStatus.createOrReplaceTempView("user_attend_status")
    //statement
    val stmt = """SELECT
        attend_user_id AS user_id,
        SUM(invited) AS invited_count,
        SUM(attended) AS attended_count,
        SUM(not_attended) AS not_attended_count,
        SUM(maybe_attended) AS maybe_attended_count
      FROM user_attend_status
      GROUP BY attend_user_id"""
    //execute
    spark.sql(stmt)
  }
  //friend attend summary
  private def getFriendAttendSummary(dfUserFriend: DataFrame, dfUserAttendStatus: DataFrame): DataFrame = {
    //register
    dfUserFriend.createOrReplaceTempView("user_friend")
    dfUserAttendStatus.createOrReplaceTempView("user_attend_status")
    //statement
    val stmt ="""
      WITH friend_attend_status AS
      (
        SELECT
          uf.user_id,
          uf.friend_id,
          uas.event_id,
          CASE WHEN uas.invited IS NOT NULL AND uas.invited > 0 THEN 1 ELSE 0 END AS invited,
          CASE WHEN uas.attended IS NOT NULL AND uas.attended > 0 THEN 1 ELSE 0 END AS attended,
          CASE WHEN uas.not_attended IS NOT NULL AND uas.not_attended > 0 THEN 1 ELSE 0 END AS not_attended,
          CASE WHEN uas.maybe_attended IS NOT NULL AND uas.maybe_attended > 0 THEN 1 ELSE 0 END AS maybe_attended
        FROM user_friend uf
          LEFT JOIN user_attend_status uas ON uf.friend_id = uas.attend_user_id
      )
      SELECT
        user_id, event_id,
        SUM(invited) AS invited_friends_count,
        SUM(attended) AS attended_friends_count,
        SUM(not_attended) AS not_attended_friends_count,
        SUM(maybe_attended) AS maybe_attended_friends_count
      FROM friend_attend_status
      WHERE event_id IS NOT NULL GROUP BY user_id, event_id"""
    //execute
    spark.sql(stmt)
  }

  //load user-friend
  private def getUsers(): DataFrame = {
    //the user-friend
    spark.read.format("org.apache.spark.sql.cassandra").option("table", "users").option("keyspace", "events_db").load()
  }
  //locale
  private def getLocale(): DataFrame = {
    //read
    spark.read
        .format("csv")
        .option("header", "false").option("delimiter", "\t")
      .load(this.sourceLocale)
      .toDF("locale_id", "locale").withColumn("locale_id", col("locale_id").cast(IntegerType))
  }

  //load train
  private def getTrain(): DataFrame = {
    //the user-friend
    spark.read.format("org.apache.spark.sql.cassandra").option("table", "train").option("keyspace", "events_db").load()
  }
  //user event count
  private def getUserEventCount(dfTrain: DataFrame): DataFrame = {
    //register
    dfTrain.createOrReplaceTempView("train")
    //statement
    val stmt = """SELECT
        user_id,
        COUNT(event_id) AS event_count
      FROM train
      GROUP BY user_id"""
    //execute
    spark.sql(stmt)
  }

  //load events
  private def getEvents(): DataFrame = {
    //the user-friend
    spark.read.format("org.apache.spark.sql.cassandra").option("table", "events").option("keyspace", "events_db").load()
  }
  //event cities
  private def getEventCityCountry(dfEvents: DataFrame, numCities: Int = 32, numCountries: Int = 8): (DataFrame, DataFrame) = {
    //register
    dfEvents.createOrReplaceTempView("events")
    //city statement
    val stmtCity = """SELECT
        t.city,
        %d + 1 - row_number() over (order by t.count desc) AS level
      FROM (SELECT city, count(*) AS count FROM events GROUP BY city ORDER BY count DESC LIMIT %d) t""".format(numCities, numCities)
    //country statement
    val stmtCountry = """SELECT
        t.country,
        %d + 1 - row_number() over (order by t.count desc) AS level
      FROM (SELECT country, count(*) AS count FROM events GROUP BY country ORDER BY count DESC LIMIT %d) t""".format(numCountries, numCountries)
    //execute
    (spark.sql(stmtCity), spark.sql(stmtCountry))
  }

  //user event
  private def getUserEvent(dfTrain: DataFrame, dfEvents: DataFrame): DataFrame = {
    //register
    dfTrain.createOrReplaceTempView("train")
    dfEvents.createOrReplaceTempView("events")
    //udf
    spark.udf.register("daysFromInvite2Start", daysFromInvite2Start)
    spark.udf.register("daysFromStartTime", daysFromStartTime)
    spark.udf.register("monthFromStartDate", monthFromStartDate)
    spark.udf.register("weekdayFromStartDate", weekdayFromStartDate)
    spark.udf.register("hourFromStartDate", hourFromStartDate)
    //sql statement
    val stmt = """
      SELECT
        t.user_id,
        t.event_id,
        t.invited AS user_invited,
        daysFromInvite2Start(t.time_stamp, e.start_time) AS invite_ahead_days,
        t.interested AS user_interested,
        e.user_id AS event_creator,
        daysFromStartTime(e.start_time) AS start_ahead_days,
        monthFromStartDate(e.start_time) AS event_start_month,
        weekdayFromStartDate(e.start_time) AS event_start_dayofweek,
        hourFromStartDate(e.start_time) AS event_start_hour,
        e.city as event_city,
        e.state as event_state,
        e.country as event_country,
        e.latitude,
        e.longitude
      FROM train t INNER JOIN events e ON t.event_id = e.event_id"""
    //execute
    spark.sql(stmt)
  }

  //build model
  private def buildModel(dfFeatureset: DataFrame): PipelineModel = {
    //make all fields Double
    val df = dfFeatureset.select(dfFeatureset.columns.map(c => col(c).cast(DoubleType)): _*)
        .withColumnRenamed("user_interested", "label")

    //get the feature columns
    val feature_columns = df.columns.filter(c => c != "label" && c != "user_id" && c != "event_id")
    //transformer
    val assembler = (new VectorAssembler()).setInputCols(feature_columns).setOutputCol("features")
    //the random forest classifier
    val rf = (new RandomForestClassifier())
      .setImpurity("gini").setMaxDepth(9).setNumTrees(30).setFeatureSubsetStrategy("auto").setSeed(21)
    //the pipeline with 2 stages & train the model
    (new Pipeline()).setStages(Array(assembler, rf)).fit(df)
  }
}
