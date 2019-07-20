package com.it21learning.process.consumers

import com.it21learning.common.{IT21Consumer, Runnable, Transferable}
import com.it21learning.config.settings.ConsumerSettings
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession

@SerialVersionUID(20180722L)
class SinkConsumer(trans: Transferable, continuousMode: Boolean = true)
    extends IT21Consumer[StringDeserializer, StringDeserializer](trans.consumerGroup, continuousMode) with Runnable[Long] with Serializable { self: ConsumerSettings =>
  //start
  def run(spark: SparkSession): Long = {
    //consume
    super.consume[String, String](trans.kafkaTopic, process(spark))
  }

  //process
  protected def process(spark: SparkSession)(kvs: Seq[(String, String)]): Unit = trans.transfer(kvs.map(x => x._2), spark)
}
