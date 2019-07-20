package com.it21learning.config.settings

trait DatabaseSettings {
  //driver
  def sinkDbDriver: String
  //url
  def sinkDbUrl: String

  //user
  def sinkDBUser: String
  //password
  def sinkDBPwd: String
}
