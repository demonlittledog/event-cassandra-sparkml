package com.it21learning.common

import java.util.{Arrays, Properties}

import com.it21learning.config.settings.ConsumerSettings

import scala.reflect.ClassTag
import scala.util.control.Breaks._
import scala.reflect.runtime.universe.{TypeTag, typeOf}
import scala.collection.JavaConversions._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords

/*
 KDS: Key Deserializer
 VDS: Value Deserializer
 */
@SerialVersionUID(20180722L)
abstract class IT21Consumer[KDS: TypeTag, VDS: TypeTag](group: String, continuousMode: Boolean) extends Serializable { self: ConsumerSettings =>
  //create consumer
  protected def create[K, V](): KafkaConsumer[K, V] = {
    //properties
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerUrl)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, group)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, typeOf[KDS].typeSymbol.fullName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, typeOf[VDS].typeSymbol.fullName)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    //create consumer
    new KafkaConsumer[K, V](props)
  }

  //consume
  def consume[K: ClassTag, V: ClassTag](topic: String, post: Seq[(K, V)] => Unit): Long = {
    //num of Records
    var numRecords: Long = 0L
    //consumer
    val consumer = create[K, V]()
    try {
      //subscribe
      consumer.subscribe(Arrays.asList(topic))

      //break block
      breakable {
        //keep polling
        while ( true ) {
          //poll
          val records: ConsumerRecords[K, V] = consumer.poll(pollingTimeout.toMillis)
          //check
          if ( records.count() > 0 ) {
            //parse
            val items: Seq[(K, V)] = {
              //loop
              for (record <- records.iterator().toList) yield (record.key(), record.value())
            }
            //post
            post(items)

            //add
            numRecords = numRecords + records.count()
          }

          //check
          if ( !continuousMode )
            break
          else {
            //sleep for 5 seconds
            Thread.sleep(5000)
          }
        }
      }
    }
    finally {
      //close
      consumer.close()
    }
    //the final # of records processed
    numRecords
  }
}
