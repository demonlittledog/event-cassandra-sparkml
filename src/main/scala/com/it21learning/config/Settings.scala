package com.it21learning.config

import com.it21learning.config.settings._
import scala.concurrent.duration._

//The application settings
object Settings {
  //debug mode
  private var __debugEnabled: Boolean = true
  //debug data dir
  private var __debugHDFSDir: String = "hdfs:///user/events/debug"

  //kafka - source
  object Source {
    //broker url
    private var _kafkaBorkerUrl: String = _
    //schema registry url
    private var _schemaRegistryUrl: Option[String] = None

    //poll interval in milli-seconds
    private var _pollingInterval: Duration = _
    //poll time out in milli-seconds
    private var _pollingTimeout: Duration = _
    //window length
    private var _windowLength: Duration = _
    //sliding interval
    private var _slidingInterval: Duration = _

    //The Producer Settings
    trait Producer extends ProducerSettings with DebugSettings {
      //broker url
      def kafkaBrokerUrl: String = _kafkaBorkerUrl
      //schema registry url
      override def schemaRegistryUrl: Option[String] = _schemaRegistryUrl

      //flag to indicate whether or not in debug mode
      override def debugEnabled: Boolean = __debugEnabled
      //the debug dir
      override def debugHDFSDir: String = __debugHDFSDir
    }
    //the Consumer Settings
    trait Consumer extends ConsumerSettings with DebugSettings {
      //broker url
      def kafkaBrokerUrl: String = _kafkaBorkerUrl
      //schema registry url
      override def schemaRegistryUrl: Option[String] = _schemaRegistryUrl

      //polling time out in milli-seconds
      override def pollingTimeout: Duration = _pollingTimeout

      //flag to indicate whether or not in debug mode
      override def debugEnabled: Boolean = __debugEnabled
      //the debug dir
      override def debugHDFSDir: String = __debugHDFSDir
    }
    //the streaming consumer
    trait StreamingConsumer extends StreamingConsumerSettings with DebugSettings {
      //broker url
      def kafkaBrokerUrl: String = _kafkaBorkerUrl
      //schema registry url
      override def schemaRegistryUrl: Option[String] = _schemaRegistryUrl

      //streaming interval in seconds
      def streamingInterval: Duration = _pollingInterval

      //the length of a window
      def windowLength: Duration = _windowLength
      //the interval
      def slidingInterval: Duration = _slidingInterval

      //flag to indicate whether or not in debug mode
      override def debugEnabled: Boolean = __debugEnabled
      //the debug dir
      override def debugHDFSDir: String = __debugHDFSDir
    }

    //load
    def initialize(): Unit = {
      //load
      this._kafkaBorkerUrl = "sandbox-hdp.hortonworks.com:6667"
      //schema registry url
      this._schemaRegistryUrl = None

      //poll interval in milli-seconds
      this._pollingInterval = Duration("5 minutes")
      this._pollingTimeout = Duration("300 milliseconds")
      this._windowLength = Duration("5 minutes")
      this._slidingInterval = Duration("2 minutes")
    }
  }

  //model settings
  object Model {
    //model directory
    private var _mlDir: String = _
    //the model load interval in seconds
    private var _mlInterval: Duration = _

    //the host
    private var _host: String = _
    //the port
    private var _port: Int = _
    //locale file
    private var _localeFile: String = _

    //train
    trait Train extends TrainSettings {
      //source
      def sourceHost: String = _host
      //the port
      def sourcePort: Int = _port
      //the locale file
      def sourceLocale: String = _localeFile

      //model sir
      def modelDir: String = _mlDir

      //flag to indicate whether or not in debug mode
      override def debugEnabled: Boolean = __debugEnabled
      //the debug dir
      override def debugHDFSDir: String = __debugHDFSDir
    }

    //the settings
    trait Settings extends ModelSettings with DebugSettings {
      //property
      def dir: String = _mlDir
      //property
      override def loadInterval: Duration = _mlInterval

      //flag to indicate whether or not in debug mode
      override def debugEnabled: Boolean = __debugEnabled
      //the debug dir
      override def debugHDFSDir: String = __debugHDFSDir
    }

    //load
    def initialize(): Unit = {
      //dir
      this._mlDir = "hdfs:///user/events/model"
      //interval
      this._mlInterval = 24 hours

      //load
      this._host = "192.168.21.11"
      this._port = 9042
      //the locale file
      this._localeFile = "/user/events/data/locale"
    }
  }

  //target - cassandra
  object Sink {
    //the host
    private var _host: String = _
    //property
    def host: String = _host

    //the port
    private var _port: Int = _
    //property
    def port: Int = _port

    //load
    def initialize(): Unit = {
      //load
      this._host = "192.168.21.11"
      this._port = 9042
    }
  }

  //target sink
  object SinkODS {
    //driver
    private var _dbDriver: String = _
    //db url
    private var _dbUrl: String = _

    //user
    private var _dbUser: String = _
    //password
    private var _dbPwd: String = _

    //the settings
    trait Settings extends DatabaseSettings {
      //property
      def sinkDbDriver: String = _dbDriver
      //property
      def sinkDbUrl: String = _dbUrl

      //property
      def sinkDBUser: String = _dbUser
      //property
      def sinkDBPwd: String = _dbPwd
    }

    //load
    def initialize(): Unit = {
      //table
      this._dbDriver = "com.mysql.jdbc.Driver"
      //load
      this._dbUrl = "jdbc:mysql://127.0.0.1:3306/events"
      //user
      this._dbUser = "root"
      //password
      this._dbPwd = "hadoop"
    }
  }

  //initialize
  def initialize(): Unit = {
    //source
    this.Source.initialize()
    //sink
    this.Sink.initialize()

    //model
    this.Model.initialize()
    //database
    this.SinkODS.initialize()

    //the debug flag
    this.__debugEnabled = false
    this.__debugHDFSDir = "hdfs:///user/events/debug"
  }
}
