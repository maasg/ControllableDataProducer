package com.gm.kafka

import java.util.HashMap
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}

// Produces String data on the provided Kafka topic
object DataProducer {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: DataProducer <metadataBrokerList> <topic> " +
        "<messagesPerSec>")
      System.exit(1)
    }

    val Array(brokers, topic, messagesPerSec) = args

    // Zookeeper connection properties
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    println ("Connected. Producer created")

    // Send some messages
    while(true) {
      val t0 = System.nanoTime
      (1 to messagesPerSec.toInt).foreach { messageNum =>
        val str = (1 to 3).map(x => scala.util.Random.nextInt(100).toString)
          .mkString(",")
        val message = new ProducerRecord[String, String](topic, null, str)
        producer.send(message)
      }
      val t1 = System.nanoTime
      val rest = Math.max(0,1000-(t1-t0)/1000000)
      println(s"resting for $rest millis" )

      Thread.sleep(rest)
    }
  }

}