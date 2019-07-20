package com.it21learning.config.settings

import org.apache.spark.sql.DataFrame

trait DebugSettings {
  //flag to indicate whether or not in debug mode
  def debugEnabled: Boolean = false

  //debug data dir
  def debugHDFSDir: String

  //write data-frame
  def write(df: DataFrame, subDir: String): Unit = {
    //check
    if ( debugEnabled ) {
      //write
      df.write.format("csv").option("header", "true").mode("overwrite").save("%s/%s".format(debugHDFSDir, subDir))
    }
  }
}
