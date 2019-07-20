package com.it21learning.sink

import org.apache.spark.sql.Row

trait SinkStatement {
  //make a statement based on the Row
  def make(value: Row): String
}
