package com.it21learning.config.settings

import scala.concurrent.duration._

trait ModelSettings {
  //property
  def dir: String

  //property
  def loadInterval: Duration = 24 hours
}
