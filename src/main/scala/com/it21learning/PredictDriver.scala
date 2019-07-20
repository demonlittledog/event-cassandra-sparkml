
package com.it21learning

import org.apache.spark.sql.SparkSession
import com.it21learning.config.Settings
import com.it21learning.process.consumers.TestConsumer

object PredictDriver {
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
      //the test streaming consumer for making predictions
      (new TestConsumer() with Settings.Source.StreamingConsumer).run(spark)
    }
    finally {
      //stop the session
      spark.stop()
    }
  }
}
