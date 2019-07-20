package com.it21learning.data

import org.apache.spark.sql.{SparkSession, DataFrame}
import com.it21learning.common.Transferable

case class UserFriend(user_id: String, friend_id: String) {

}

object UserFriend extends Transferable {
  //kafka topic
  def kafkaTopic: String = "user_friends"
  //consumer group
  def consumerGroup: String = "grpUserFriends"

  //cassandra table
  def tableName: String = "user_friend"

  //build
  def construct(rows: Seq[String], spark: SparkSession): DataFrame = {
    //create the objects
    val userFriends: Seq[UserFriend] = rows.map(s => s.split(",", -1))
      .map(f => UserFriend(f(0), f(1)))
    //create
    import spark.implicits._
    spark.createDataset(userFriends).toDF
  }
}