package com.it21learning.config.settings

import scala.concurrent.duration.Duration

trait ConsumerSettings extends KafkaSettings {
  //polling time out in milli-seconds
  def pollingTimeout: Duration = Duration("300 milliseconds")
}
