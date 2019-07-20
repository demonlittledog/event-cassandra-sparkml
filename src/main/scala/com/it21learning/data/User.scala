package com.it21learning.data

import org.apache.spark.sql.{SparkSession, DataFrame}
import com.it21learning.common.Transferable

case class User(user_id: String, birth_year: String, gender: String, locale: String, location: String, time_zone: String, joined_at: String) {

}

object User extends Transferable {
  //kafka topic
  def kafkaTopic: String = "users"
  //consumer group
  def consumerGroup: String = "grpUsers"

  //cassandra table
  def tableName: String = "users"

  //build
  def construct(rows: Seq[String], spark: SparkSession): DataFrame = {
    //create the objects
    val users: Seq[User] = rows.map(s => s.split(",", -1))
      .map(f => User(f(0), f(2), f(3), f(1), f(5), f(6), f(4)))
    //create
    import spark.implicits._
    spark.createDataset(users).toDF
  }
}