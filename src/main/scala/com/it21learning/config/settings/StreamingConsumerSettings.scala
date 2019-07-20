package com.it21learning.config.settings

import scala.concurrent.duration.Duration

trait StreamingConsumerSettings extends KafkaSettings with DebugSettings {
  //streaming interval in seconds
  def streamingInterval: Duration

  //the length of a window
  def windowLength: Duration
  //the interval
  def slidingInterval: Duration
}
