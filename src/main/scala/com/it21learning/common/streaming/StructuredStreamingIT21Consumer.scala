package com.it21learning.common.streaming

import com.it21learning.config.settings.StreamingConsumerSettings
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}

@SerialVersionUID(20180722L)
class StructuredStreamingIT21Consumer(process: DataFrame => DataFrame) extends Serializable { self: StreamingConsumerSettings =>
  //the output mode
  object OutputMode extends Enumeration {
    type OutputMode = Value
    val append, update, complete = Value
  }

  //start
  def start(spark: SparkSession, topic: String, sink: ForeachWriter[Row], mode: OutputMode.Value): Unit = {
    //load
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokerUrl).option("subscribe", topic)
      .load()
    //output mode
    val output = mode match {
      case OutputMode.append => "append"
      case OutputMode.update => "update"
      case _ => "complete"
    }
    //cast & process
    val query = process(df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)"))
      .writeStream
      .foreach(sink).outputMode(output)
      .trigger(ProcessingTime(this.streamingInterval*1000L))
      .start()
    //wait
    query.awaitTermination()
  }
}
