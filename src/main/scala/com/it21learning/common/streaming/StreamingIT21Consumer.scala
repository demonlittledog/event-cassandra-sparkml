package com.it21learning.common.streaming

import com.it21learning.config.settings.StreamingConsumerSettings
import org.apache.avro.SchemaBuilder.ArrayBuilder
import org.apache.hadoop.util.hash.Hash
import org.apache.kafka.clients.consumer.ConsumerConfig

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{TypeTag, typeOf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/*
 KDS: Key Deserializer
 VDS: Value Deserializer
 */
@SerialVersionUID(20180722L)
abstract class StreamingIT21Consumer[KDS: TypeTag, VDS: TypeTag](group: String) extends Serializable { self: StreamingConsumerSettings =>
  //topic
  def topic: String

  //start
  def start[K: ClassTag, V: ClassTag](spark: SparkSession, process: (SparkSession, RDD[(K, V)]) => Unit): Unit = {
    //the stream context
    if ( spark == null || spark.sparkContext == null )
      throw new Exception("The context is NULL")
    if ( streamingInterval == null )
      throw new Exception("The interval is NULL")

    val ssCtx = new StreamingContext(spark.sparkContext, Duration(streamingInterval.toMillis))
    try {
      //create the stream
      create[K, V](ssCtx, topic, group)
        .foreachRDD(rdd => process(spark, rdd.map { cr: ConsumerRecord[K, V] => (cr.key(), cr.value()) }))

      //start
      ssCtx.start()
      //run till terminated
      ssCtx.awaitTermination()
    }
    finally {
      //close
      ssCtx.stop()
    }
  }

  //window
  def window[K: ClassTag, V: ClassTag](spark: SparkSession, process: (SparkSession, RDD[(K, V)]) => Unit): Unit = {
    //the stream context
    val ssCtx = new StreamingContext(spark.sparkContext, Duration(streamingInterval.toMillis))
    try {
      //create the stream
      create[K, V](ssCtx, topic, group)
        .map { cr: ConsumerRecord[K, V] => (cr.key(), cr.value()) }
        .window(Duration(windowLength.toMillis), Duration(slidingInterval.toMillis))
        .cache()
        .foreachRDD(rdd => process(spark, rdd))

      //start
      ssCtx.start()
      //run till terminated
      ssCtx.awaitTermination()
    }
    finally {
      //close
      ssCtx.stop()
    }
  }

  //create input-stream
  private def create[K: ClassTag, V: ClassTag](ssc: StreamingContext, topic: String, group: String): InputDStream[ConsumerRecord[K, V]] = {
    //parameters
    val initKafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaBrokerUrl,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> typeOf[KDS].typeSymbol.fullName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> typeOf[VDS].typeSymbol.fullName,
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest")
    //check
    val kafkaParams = this.schemaRegistryUrl match {
      case Some(url) => {
        import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
        initKafkaParams ++ Map(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG -> "true", "schema.registry.url" -> schemaRegistryUrl)
      }
      case _ => initKafkaParams
    }
    //check
    if ( this.debugEnabled ) {
      //reset offset
      resetOffset(kafkaParams)
    }
    //create the stream poll
    KafkaUtils.createDirectStream[K, V](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Assign[K, V](Seq(new TopicPartition(topic, 0)), kafkaParams))
  }

  //reset topic(s) offset for tests
  private def resetOffset(kafkaParams: Map[String, Object]): Unit = {
    //convert to properties
    import java.util.Properties
    val properties = (new Properties /: kafkaParams) {
      case (p, (k, v)) => p.put(k,v); p
    }
    //check the auto-offset-reset strategy
    import org.apache.kafka.clients.consumer.KafkaConsumer
    import java.util.Arrays
    //create a consumer
    val consumer = new KafkaConsumer[String, String](properties)
    try {
      //subscribe
      consumer.subscribe(Arrays.asList[String](Array(this.topic): _*))
      //dump poll
      consumer.poll(100L)
      //set the beginning
      consumer.seekToBeginning(Arrays.asList(Array(new TopicPartition(this.topic, 0)): _*))
    }
    finally {
      //close
      consumer.close()
    }
  }
}
