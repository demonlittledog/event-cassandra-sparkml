package com.it21learning.data

import com.it21learning.sink.SinkStatement
import org.apache.spark.sql.Row

object Prediction extends SinkStatement {
  //table
  private val tbl: String = "predictions"
  //the insert statement
  private val fmt: String = s"INSERT INTO $tbl(user, event, prediction) VALUES('%s', '%s', %d)"

  //make a statement based on the Row
  def make(value: Row): String = fmt.format(value.getString(0), value.getString(1), value.getInt(2))
}
