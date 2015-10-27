package com.gm

import akka.actor.{Props, ActorSystem}
import com.gm.kafka.DataProducer
import com.gm.serial.ControlActor

object LoadGen {
  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      args.zipWithIndex.foreach(println _)
      System.err.println(s"Usage: DataProducer <metadataBrokerList> <topic>. was: $args with size ${args.size}")
      System.exit(1)
    }

    val Array(brokers, topics) = args

    val system: akka.actor.ActorSystem = ActorSystem("CommSystem")

    val dataProducer = system.actorOf(Props(new DataProducer(brokers, topics)), name = "kafkaProducerActor")

    val controlActor = system.actorOf(Props(new ControlActor(dataProducer)), name = "commcontrolactor")

  }

}
