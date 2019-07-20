package com.it21learning.process.consumers

import java.text.SimpleDateFormat
import java.util.Date
import com.it21learning.config.settings.ConsumerSettings
import com.it21learning.data.Train
import com.it21learning.data.Train.{keySpace, tableName}
import org.apache.spark.sql.SparkSession

class TrainConsumer(continuousMode: Boolean) extends SinkConsumer(Train, continuousMode) { self: ConsumerSettings =>
  //process
  override protected def process(spark: SparkSession)(kvs: Seq[(String, String)]): Unit = {
    //call super
    super.process(spark)(kvs)

    //check
    if ( kvs.length > 0 ) {
      //time-stamp
      val timeStamp: String = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date())
      //update
      spark.createDataFrame(Seq((timeStamp, 0, 1))).toDF("time_stamp", "done", "counter_value")
        .write
          .format("org.apache.spark.sql.cassandra")
          .option("table", "train_update").option("keyspace", keySpace)
          .mode("append")
        .save()
    }
  }
}
