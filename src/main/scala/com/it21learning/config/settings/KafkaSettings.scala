package com.it21learning.config.settings

@SerialVersionUID(20180722L)
trait KafkaSettings extends Serializable {
  //broker url
  def kafkaBrokerUrl: String

  //schema registry url
  def schemaRegistryUrl: Option[String] = None
}
