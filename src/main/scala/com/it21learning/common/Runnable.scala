package com.it21learning.common

import org.apache.spark.sql.SparkSession

trait Runnable[T] {
  //method
  def run(spark: SparkSession): T
}
