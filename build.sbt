name := "kafkaproducer"

version := "0.1"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.8.2.2" exclude("javax.jms", "jms")

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.5.1"
    
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.5.1"

