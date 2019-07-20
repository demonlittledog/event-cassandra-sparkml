package com.it21learning.common

import java.util.Properties

import com.it21learning.config.settings.ProducerSettings

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{TypeTag, typeOf}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

/*
  KS: KeySerializer
  VS: ValueSerializer
 */
@SerialVersionUID(20180722L)
abstract class IT21Producer[KS: TypeTag, VS: TypeTag](client: String) extends Serializable { this: ProducerSettings =>
  //create a Kafka Producer
  protected def create[K, V](): KafkaProducer[K, V] = {
    //properties
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerUrl)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, client)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, typeOf[KS].typeSymbol.fullName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, typeOf[VS].typeSymbol.fullName)
    //create
    new KafkaProducer[K, V](props)
  }

  //produce
  //K -> Key type
  //V -> Value type
  def produce[K: ClassTag, V: ClassTag](topic: String, items: Seq[(K, V)]): Unit = {
    //create producer
    val producer = create[K, V]()
    try {
      //loop & send
      items foreach { case(k: K, v: V) => producer.send(new ProducerRecord[K, V](topic, k, v)) }

      //flush all content
      producer.flush()
    }
    finally {
      //close
      producer.close()
    }
  }
}
