package com.it21learning.process

import java.util.Date
import scala.util.{Success, Try}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/* -----------------------------------------------------------------------------------------------------------

 * The following code is created by Wayne Shen for the Big Data Training
 *
 * Date: Sept. 29, 2018
 * Copyright @ iT21 Learning
 *
 ---------------------------------------------------------------------------------------------------------- */
@SerialVersionUID(20180722L)
class EventTransformer(spark: SparkSession) extends Serializable {
  //event start-time format
  protected val fmtEventStartTime = "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z"
  //invite time-stamp format
  protected val fmtInviteTimeStamp = "^\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}.*"
  //joined at time-stamp
  protected val fmtJoinTimeStamp = "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z"

  //the date-time format
  private val fmtDateTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  //month format
  private val fmtMonth = new java.text.SimpleDateFormat("MM")
  //day of week format
  private val fmtDayOfWeek = new java.text.SimpleDateFormat("u")
  //hour format
  private val fmtHour = new java.text.SimpleDateFormat("HH")
  //year format
  private val fmtYear = new java.text.SimpleDateFormat("YYYY")

  //calculate the date
  private def calculateDate(dt: String, fmt: String): Option[Date] = {
    //check
    dt match {
      case fmt.r(year, month, day, hour, minute, second) => Try(fmtDateTime.parse("%s-%s-%s %s:%s:%s".format(year, month, day, hour, minute, second))).toOption
      case _ => None
    }
  }
  //calculate the month
  private def calculateMonth(dt: String, fmt: String): Option[Int] = {
    //check
    calculateDate(dt, fmt) match {
      case Some(date) => Try(fmtMonth.format(date).toInt).toOption
      case _ => None
    }
  }
  //calculate the hours
  private def calculateHour(dt: String, fmt: String): Option[Int] = {
    //check
    calculateDate(dt, fmt) match {
      case Some(date) => Try(fmtHour.format(date).toInt).toOption
      case _ => None
    }
  }
  //calculate the hours
  private def calculateDayOfWeek(dt: String, fmt: String): Option[Int] = {
    //check
    calculateDate(dt, fmt) match {
      case Some(date) => Try(fmtDayOfWeek.format(date).toInt).toOption
      case _ => None
    }
  }

  //calculate the days
  private def calculateDays(dtStart: String, fmtStart: String, end: Option[(String, String)] = None): Option[Int] = {
    //start
    calculateDate(dtStart, fmtStart) match {
      case Some(dateStart) => {
        //check the end
        val endDate = end match {
          case Some((dtEnd, fmtEnd)) => calculateDate(dtEnd, fmtEnd)
          case _ => Some(new Date())
        }

        //check
        endDate match {
          case Some(dateEnd) => Some(((dateEnd.getTime() - dateStart.getTime())/(24*60*60*1000) + 1).toInt)
          case _ => None
        }
      }
      case _ => None
    }
  }

  //calculate the days from event start-time
  protected val daysFromStartTime = udf { (dt: String) => calculateDays(dt, fmtEventStartTime) }
  //calculate the start month, week-of-day, and hour
  protected val startFacts = udf {
    (dt: String) => {
      var month: Option[Int] = None
      var dayOfWeek: Option[Int] = None
      var hour: Option[Int] = None

      //check
      dt.matches(fmtEventStartTime) match {
        case true => {
          //the date
          Try(fmtDateTime.parse("%s %s".format(dt.substring(0, 10), dt.substring(11, 19)))) match {
            case Success(date) => {
              //organize the result
              month = Try(fmtMonth.format(date).toInt).toOption
              dayOfWeek = Try(fmtDayOfWeek.format(date).toInt).toOption
              hour = Try(fmtHour.format(date).toInt).toOption
            }
            case _ => { /* do nothing */ }
          }
        }
        case _ => { /* do notning */ }
      }
      Array(month, dayOfWeek, hour)
    }
  }
  //calculate the days from invite time-stamp
  protected val daysFromInviteTime = udf { (dt: String) => calculateDays(dt, fmtInviteTimeStamp) }
  //calculate the days from event start-time
  protected val daysFromJoinTime = udf { (dt: String) => calculateDays(dt, fmtJoinTimeStamp) }
  //calculate the days from invite to start
  protected val daysFromInvite2Start = udf { (dtInvite: String, dtStart: String) => calculateDays(dtStart, fmtInviteTimeStamp, Some((dtInvite, fmtEventStartTime))) }
  //month
  protected val monthFromStartDate = udf { (dt: String) => calculateMonth(dt, fmtEventStartTime) }
  //week of day
  protected val weekdayFromStartDate = udf { (dt: String) => calculateDayOfWeek(dt, fmtEventStartTime) }
  //hour
  protected val hourFromStartDate = udf { (dt: String) => calculateHour(dt, fmtEventStartTime) }

  //check if locations are similar
  protected val locationSimilar = udf {
    (user_location: String, event_city: String, event_province: String, event_country: String) => if (user_location.contains(event_city) || user_location.contains(event_province) || user_location.contains(event_country)) 1 else 0
  }

  //event creator is friend
  protected def getEventCreatorIsFriend(dfUserEvent: DataFrame, tblUserFriend: String, tblUserFriendCount: String): DataFrame = {
    //register table
    dfUserEvent.createOrReplaceTempView("user_event")
    //the user-friend
    spark.read
        .format("org.apache.spark.sql.cassandra")
        .option("table", tblUserFriend).option("keyspace", "events_db")
      .load()
      .createOrReplaceTempView(tblUserFriend)
    //the user-friend-count
    spark.read
      .format("org.apache.spark.sql.cassandra")
      .option("table", tblUserFriendCount).option("keyspace", "events_db")
      .load()
      .createOrReplaceTempView(tblUserFriendCount)
    //sql statement
    val stmt: String = s"""SELECT
        ue.user_id,
        ue.event_id,
        ue.user_invited,
        ue.invite_ahead_days,
        ue.user_interested,
        ue.event_creator,
        ue.start_ahead_days,
        ue.event_start_month,
        ue.event_start_dayofweek,
        ue.event_start_hour,
        ue.event_city,
        ue.event_state,
        ue.event_country,
        ue.latitude,
        ue.longitude,
        CASE WHEN uf.friend_id IS NOT NULL THEN 1 ELSE 0 END AS event_creator_is_friend,
        CASE WHEN ufc.friend_count IS NOT NULL THEN ufc.friend_count ELSE 0 END AS user_friend_count
      FROM user_event ue
        LEFT JOIN $tblUserFriend uf ON ue.user_id = uf.user_id AND ue.event_creator = uf.friend_id
        LEFT JOIN $tblUserFriendCount ufc ON ue.user_id = ufc.user_id"""
    //execute
    spark.sql(stmt)
  }

  //get the friend attend percentage
  protected def getFriendAttendPercentage(dfEventCreatorIsFriend: DataFrame, tblFriendAttendSummary: String): DataFrame = {
    //register
    dfEventCreatorIsFriend.createOrReplaceTempView("event_creator_is_friend")
    //the user-friend-count
    spark.read
        .format("org.apache.spark.sql.cassandra")
        .option("table", tblFriendAttendSummary).option("keyspace", "events_db")
      .load()
      .createOrReplaceTempView(tblFriendAttendSummary)
    //statement
    val stmt: String = s"""SELECT
        ecif.user_id,
        ecif.event_id,
        ecif.invite_ahead_days,
        ecif.start_ahead_days,
        ecif.event_start_month,
        ecif.event_start_dayofweek,
        ecif.event_start_hour,
        ecif.event_city,
        ecif.event_state,
        ecif.event_country,
        ecif.latitude,
        ecif.longitude,
        ecif.event_creator_is_friend,
        ecif.user_friend_count,
        ecif.user_invited,
        ecif.user_interested,
        CASE WHEN fas.invited_friends_count IS NOT NULL THEN fas.invited_friends_count ELSE 0 END AS invited_friends_count,
        CASE WHEN fas.attended_friends_count IS NOT NULL THEN fas.attended_friends_count ELSE 0 END AS attended_friends_count,
        CASE WHEN fas.not_attended_friends_count IS NOT NULL THEN fas.not_attended_friends_count ELSE 0 END AS not_attended_friends_count,
        CASE WHEN fas.maybe_attended_friends_count IS NOT NULL THEN fas.maybe_attended_friends_count ELSE 0 END AS maybe_attended_friends_count,
        CASE WHEN ecif.user_friend_count != 0 AND fas.invited_friends_count IS NOT NULL THEN fas.invited_friends_count * 100 / ecif.user_friend_count ELSE 0 END as invited_friends_percentage,
        CASE WHEN ecif.user_friend_count != 0 AND fas.attended_friends_count IS NOT NULL THEN fas.attended_friends_count * 100 / ecif.user_friend_count ELSE 0 END as attended_friends_percentage,
        CASE WHEN ecif.user_friend_count != 0 AND fas.not_attended_friends_count IS NOT NULL THEN fas.not_attended_friends_count * 100 / ecif.user_friend_count ELSE 0 END as not_attended_friends_percentage,
        CASE WHEN ecif.user_friend_count != 0 AND fas.maybe_attended_friends_count IS NOT NULL THEN fas.maybe_attended_friends_count * 100 / ecif.user_friend_count ELSE 0 END as maybe_attended_friends_percentage
      FROM event_creator_is_friend ecif
        LEFT JOIN $tblFriendAttendSummary fas ON ecif.user_id = fas.user_id AND ecif.event_id = fas.event_id"""
    //execute
    spark.sql(stmt)
  }

  //get user-friend-event
  protected def getUserFriendEvent(dfFriendAttendPercentage: DataFrame, dfUserEventCount: DataFrame,
      tblUsers: String, tblEventAttendeeCount: String, tblUserAttendEventCount: String,
      tblEventCities: String, tblEventCountries: String, tblLocale: String): DataFrame = {
    //register
    dfFriendAttendPercentage.createOrReplaceTempView("friend_attend_percentage")
    dfUserEventCount.createOrReplaceTempView("user_event_count")
    //the users
    spark.read
        .format("org.apache.spark.sql.cassandra")
        .option("table", tblUsers).option("keyspace", "events_db")
      .load()
      .createOrReplaceTempView(tblUsers)
    //the event-attendee-count
    spark.read
        .format("org.apache.spark.sql.cassandra")
        .option("table", tblEventAttendeeCount).option("keyspace", "events_db")
      .load()
      .createOrReplaceTempView(tblEventAttendeeCount)
    //the event-attendee-count
    spark.read
        .format("org.apache.spark.sql.cassandra")
        .option("table", tblUserAttendEventCount).option("keyspace", "events_db")
      .load()
      .createOrReplaceTempView(tblUserAttendEventCount)
    //the event-cities
    spark.read
      .format("org.apache.spark.sql.cassandra")
      .option("table", tblEventCities).option("keyspace", "events_db")
      .load()
      .createOrReplaceTempView(tblEventCities)
    //the event-attendee-count
    spark.read
      .format("org.apache.spark.sql.cassandra")
      .option("table", tblEventCountries).option("keyspace", "events_db")
      .load()
      .createOrReplaceTempView(tblEventCountries)
    //the event-attendee-count
    spark.read
      .format("org.apache.spark.sql.cassandra")
      .option("table", tblLocale).option("keyspace", "events_db")
      .load()
      .createOrReplaceTempView(tblLocale)
    //register udf
    spark.udf.register("age", (birth_year: Int) => fmtYear.format(new Date()).toInt - birth_year)
    spark.udf.register("daysFromJoinTime", daysFromJoinTime)
    spark.udf.register("locationSimilar", locationSimilar)
    //statement
    val stmt: String = s"""SELECT
        CAST(fap.user_interested AS int) AS user_interested,
        fap.user_id,
        fap.event_id,
        CASE WHEN l.locale_id IS NOT NULL THEN l.locale_id ELSE 0 END AS user_locale,
        CASE WHEN u.gender = 'male' THEN 1 WHEN u.gender = 'female' THEN 0 ELSE -1 END AS user_gender,
        age(u.birth_year) AS user_age,
        CAST(u.time_zone AS int) AS user_time_zone,
        daysFromJoinTime(u.joined_at) AS user_member_years,
        fap.user_friend_count,
        fap.invite_ahead_days AS user_invite_ahead_days,
        uec.event_count AS user_had_event_count,
        uaec.invited_count AS user_invited_event_count,
        uaec.attended_count AS user_attended_event_count,
        uaec.not_attended_count AS user_not_attended_event_count,
        uaec.maybe_attended_count AS user_maybe_attended_event_count,
        CAST(fap.user_invited AS int) AS user_invited,
        fap.invited_friends_count AS user_invited_friends_count,
        fap.attended_friends_count AS user_attended_friends_count,
        fap.not_attended_friends_count AS user_not_attended_friends_count,
        fap.maybe_attended_friends_count AS user_maybe_attended_friends_count,
        fap.invited_friends_percentage AS user_invited_friends_percentage,
        fap.attended_friends_percentage AS user_attended_friends_percentage,
        fap.not_attended_friends_percentage AS user_not_attended_friends_percentage,
        fap.maybe_attended_friends_percentage AS user_maybe_attended_friends_percentage,
        fap.start_ahead_days AS event_start_ahead_days,
        fap.event_start_month,
        CAST(fap.event_start_dayofweek AS int) AS event_start_dayofweek,
        fap.event_start_hour,
        iv.attend_count AS event_invited_user_count,
        yes.attend_count AS event_attended_user_count,
        no.attend_count AS event_not_attended_user_count,
        maybe.attend_count AS event_maybe_attended_user_count,
        ec.level AS event_city_level,
        et.level AS event_country_level,
        fap.event_creator_is_friend,
        locationSimilar(lower(u.location), lower(fap.event_city), lower(fap.event_state), lower(fap.event_country)) AS location_similar
        FROM friend_attend_percentage fap
          INNER JOIN $tblUsers u ON fap.user_id = u.user_id
          INNER JOIN user_event_count uec ON uec.user_id = fap.user_id
          INNER JOIN $tblEventAttendeeCount iv ON iv.event_id = fap.event_id AND iv.attend_type = 'invited'
          INNER JOIN $tblEventAttendeeCount yes ON yes.event_id = fap.event_id AND yes.attend_type = 'yes'
          INNER JOIN $tblEventAttendeeCount no ON no.event_id = fap.event_id AND no.attend_type = 'no'
          INNER JOIN $tblEventAttendeeCount maybe ON maybe.event_id = fap.event_id AND maybe.attend_type = 'maybe'
          LEFT JOIN $tblUserAttendEventCount uaec ON uaec.user_id = fap.user_id
          LEFT JOIN $tblEventCities ec ON ec.city = fap.event_city
          LEFT JOIN $tblEventCountries et ON et.country = fap.event_country
          LEFT JOIN $tblLocale l ON u.locale = l.locale"""
    //execute
    spark.sql(stmt)
  }

  //prepare feature set
  protected def prepareFeatureSet(dfUserFriendEvent: DataFrame): DataFrame = {
    //register
    dfUserFriendEvent.createOrReplaceTempView("user_friend_event")
    //statement
    val stmt ="""SELECT
        CASE WHEN m.user_interested IS NULL THEN 0 ELSE m.user_interested END AS user_interested,
        m.user_id,
        m.event_id,
        CASE WHEN m.user_locale IS NULL THEN 0 ELSE m.user_locale END AS user_locale,
        CASE WHEN m.user_gender IS NULL THEN -1 ELSE m.user_gender END AS user_gender,
        CASE WHEN m.user_age IS NULL THEN s.average_age ELSE m.user_age END AS user_age,
        CASE WHEN m.user_time_zone IS NULL THEN 0 ELSE m.user_time_zone END AS user_time_zone,
        CASE WHEN m.user_member_years IS NULL THEN 0 ELSE m.user_member_years END AS user_member_years,
        CASE WHEN m.user_friend_count IS NULL THEN 0 ELSE m.user_friend_count END AS user_friend_count,
        CASE WHEN m.user_invite_ahead_days IS NULL THEN 0 ELSE m.user_invite_ahead_days END AS user_invite_ahead_days,
        CASE WHEN m.user_had_event_count IS NULL THEN 0 ELSE m.user_had_event_count END AS user_had_event_count,
        CASE WHEN m.user_invited_event_count IS NULL THEN 0 ELSE m.user_invited_event_count END AS user_invited_event_count,
        CASE WHEN m.user_attended_event_count IS NULL THEN 0 ELSE m.user_attended_event_count END AS user_attended_event_count,
        CASE WHEN m.user_not_attended_event_count IS NULL THEN 0 ELSE m.user_not_attended_event_count END AS user_not_attended_event_count,
        CASE WHEN m.user_maybe_attended_event_count IS NULL THEN 0 ELSE m.user_maybe_attended_event_count END AS user_maybe_attended_event_count,
        CASE WHEN m.user_invited IS NULL THEN 0 ELSE m.user_invited END AS user_invited,
        CASE WHEN m.user_invited_friends_count IS NULL THEN 0 ELSE m.user_invited_friends_count END AS user_invited_friends_count,
        CASE WHEN m.user_attended_friends_count IS NULL THEN 0 ELSE m.user_attended_friends_count END AS user_attended_friends_count,
        CASE WHEN m.user_not_attended_friends_count IS NULL THEN 0 ELSE m.user_not_attended_friends_count END AS user_not_attended_friends_count,
        CASE WHEN m.user_maybe_attended_friends_count IS NULL THEN 0 ELSE m.user_maybe_attended_friends_count END AS user_maybe_attended_friends_count,
        CASE WHEN m.user_invited_friends_percentage IS NULL THEN 0 ELSE m.user_invited_friends_percentage END AS user_invited_friends_percentage,
        CASE WHEN m.user_attended_friends_percentage IS NULL THEN 0 ELSE m.user_attended_friends_percentage END AS user_attended_friends_percentage,
        CASE WHEN m.user_not_attended_friends_percentage IS NULL THEN 0 ELSE m.user_not_attended_friends_percentage END AS user_not_attended_friends_percentage,
        CASE WHEN m.user_maybe_attended_friends_percentage IS NULL THEN 0 ELSE m.user_maybe_attended_friends_percentage END AS user_maybe_attended_friends_percentage,
        CASE WHEN m.event_start_ahead_days IS NULL THEN 0 ELSE m.event_start_ahead_days END AS event_start_ahead_days,
        CASE WHEN m.event_start_month IS NULL THEN 0 ELSE m.event_start_month END AS event_start_month,
        CASE WHEN m.event_start_dayofweek IS NULL THEN 0 ELSE m.event_start_dayofweek END AS event_start_dayofweek,
        CASE WHEN m.event_start_hour IS NULL THEN 0 ELSE m.event_start_hour END AS event_start_hour,
        CASE WHEN m.event_invited_user_count IS NULL THEN 0 ELSE m.event_invited_user_count END AS event_invited_user_count,
        CASE WHEN m.event_attended_user_count IS NULL THEN 0 ELSE m.event_attended_user_count END AS event_attended_user_count,
        CASE WHEN m.event_not_attended_user_count IS NULL THEN 0 ELSE m.event_not_attended_user_count END AS event_not_attended_user_count,
        CASE WHEN m.event_maybe_attended_user_count IS NULL THEN 0 ELSE m.event_maybe_attended_user_count END AS event_maybe_attended_user_count,
        CASE WHEN m.event_city_level IS NULL THEN 0 ELSE m.event_city_level END AS event_city_level,
        CASE WHEN m.event_country_level IS NULL THEN 0 ELSE m.event_country_level END AS event_country_level,
        CASE WHEN m.event_creator_is_friend IS NULL THEN 0 ELSE m.event_creator_is_friend END AS event_creator_is_friend,
        CASE WHEN m.location_similar IS NULL THEN 0 ELSE m.location_similar END AS location_similar
          FROM user_friend_event m
          CROSS JOIN (SELECT AVG(user_age) AS average_age FROM user_friend_event) s"""
    //execute
    spark.sql(stmt)
  }
}
