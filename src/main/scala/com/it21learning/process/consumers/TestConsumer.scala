package com.it21learning.process.consumers

import com.it21learning.config.settings.{DebugSettings, StreamingConsumerSettings}
import com.it21learning.common.streaming.StreamingIT21Consumer
import com.it21learning.common.Runnable
import com.it21learning.process.EventPredictor
import com.it21learning.config.Settings
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

@SerialVersionUID(20180722L)
class TestConsumer extends StreamingIT21Consumer[StringDeserializer, StringDeserializer]("grpTests") with Runnable[Unit] with Serializable { self: StreamingConsumerSettings =>
  //topic
  def topic: String = "test"

  //the event predictor
  private var eventPredictor: Option[EventPredictor] = None

  //start
  def run(spark: SparkSession): Unit = {
    //check
    if (this.eventPredictor == None) {
      //check
      val debug: Option[DebugSettings] = if (this.debugEnabled) Some(this) else None
      //create
      this.eventPredictor = Some(new EventPredictor(spark, debug) with Settings.SinkODS.Settings)
    }
    //case base
    super.start[String, String](spark, process)
  }

  //process the RDD
  private def process(spark: SparkSession, rdd: RDD[(String, String)]): Unit = {
    //check
    this.eventPredictor match {
      case Some(predictor) => predictor.process(rdd.map(r => r._2))
      case _ => throw new RuntimeException("The prediction is not instantiated.")
    }
  }
}
