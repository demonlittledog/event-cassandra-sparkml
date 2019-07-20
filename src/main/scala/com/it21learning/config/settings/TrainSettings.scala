package com.it21learning.config.settings

trait TrainSettings extends DebugSettings {
  //source
  def sourceHost: String
  //the port
  def sourcePort: Int

  //the locale file
  def sourceLocale: String

  //model sir
  def modelDir: String
}
