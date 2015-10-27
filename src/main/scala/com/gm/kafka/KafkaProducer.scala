package com.gm.kafka

import java.util.HashMap
import akka.actor.{ActorLogging, Actor}
import scala.concurrent.duration._
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}

// Produces String data on the provided Kafka topic
class DataProducer(brokers: String, topic:String) extends Actor with ActorLogging {
  import DataProducer._

  // Zookeeper connection properties
  val props = new HashMap[String, Object]()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)
  println ("Connected. Producer created")

  var msgsPerSec = MessagesPerSecond(0)

  var counter = 0L

  val tempFunc: Long => Double = l => 20 + 10 * Math.cos(l) + scala.util.Random.nextGaussian()*10-5
  val humFunc:  Long => Double = l => 50 + 20 * Math.sin(l) + scala.util.Random.nextGaussian()*5-5

  import context.dispatcher
  val tick =  context.system.scheduler.schedule(1000 millis , 1000 millis, self, Run)

  override def postStop() = tick.cancel()

  def receive = {
    case Run =>
      val t0 = System.nanoTime
      (1 to msgsPerSec.value).foreach { messageNum =>
        val str = s"${tempFunc(counter)}, ${humFunc(counter)}"
        val message = new ProducerRecord[String, String](topic, null, str)
        producer.send(message)
      }
      val t1 = System.nanoTime
      val restTime = Math.max(0,1000-(t1-t0)/1000000)
      println(s"Time Left: $restTime" )

    case newRate:MessagesPerSecond => msgsPerSec = newRate
      println( s"new rate received: $newRate")

  }

}
object DataProducer {
  case class MessagesPerSecond(val value: Int)
  case object Run
}
