package com.it21learning

import org.apache.spark.sql.SparkSession
import com.it21learning.config.Settings
import com.it21learning.process.ModelTrainer

object TrainDriver {
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
      (new ModelTrainer(spark) with Settings.Model.Train with Settings.SinkODS.Settings).train()
    }
    finally {
      //stop the session
      spark.stop()
    }
  }
}
