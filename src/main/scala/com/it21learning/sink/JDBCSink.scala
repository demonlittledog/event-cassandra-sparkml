package com.it21learning.sink

import java.sql.{Connection, DriverManager, Statement}

import com.it21learning.config.settings.DatabaseSettings
import org.apache.spark.sql.{ForeachWriter, Row}

@SerialVersionUID(20180722L)
class JDBCSink(db: DatabaseSettings)
    extends ForeachWriter[Row] with Serializable { self: SinkStatement =>
  //the driver
  val driver: String = "com.mysql.jdbc.Driver"
  //the connection object
  var connection: Connection = _
  //the sql statement
  var statement: Statement = _

  //open
  def open(partitionId: Long,version: Long): Boolean = {
    //load the class
    Class.forName(driver)
    //create the connection
    connection = DriverManager.getConnection(db.sinkDbUrl, db.sinkDBUser, db.sinkDBPwd)
    //the statement
    statement = connection.createStatement
    //return
    true
  }

  //execute
  def process(value: Row): Unit = {
    //execute
    statement.executeUpdate(this.make(value))
  }

  //close
  def close(errorOrNull: Throwable): Unit = {
    //close the connection
    connection.close
  }
}
