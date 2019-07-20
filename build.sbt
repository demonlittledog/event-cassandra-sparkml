
name := "event-ml-cassandra"

version := "0.1"

scalaVersion := "2.11.11"

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", "unused", xs @ _*) => MergeStrategy.first
  case PathList("org", "tukaani", "xz", xs @ _*) => MergeStrategy.first
  case PathList("org", "codehaus", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", "commons", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", "kafka", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", "avro", xs @ _*) => MergeStrategy.first
  case PathList("scala", "util", "parsing", xs @ _*) => MergeStrategy.first
  case PathList("org", "I0Itec", "zkclient", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", "metrics", xs @ _*) => MergeStrategy.last
  case PathList("net", "jpountz", xs @ _*) => MergeStrategy.last
  case PathList("kafka", xs @ _*) => MergeStrategy.last
  case PathList("com", "thoughtworks", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.contains("X86.class") => MergeStrategy.first
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.2" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.2.2" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.2.2" % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.2.2" % "provided",
  "org.apache.kafka" %% "kafka" % "0.10.2.2",
  "org.apache.kafka" % "kafka-clients" % "0.10.2.2",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.2.2",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.2.2",
  "org.apache.avro" % "avro" % "1.8.1",
  "io.confluent" % "kafka-avro-serializer" % "4.1.1"
)