package com.gm.serial

import com.github.jodersky.flow.{Parity, Serial, SerialSettings, AccessDeniedException}

import akka.actor._
import akka.io.IO
import akka.util.ByteString
import com.gm.kafka.DataProducer

import scala.collection.mutable.ArrayBuffer

class ControlActor(dataProducer: ActorRef) extends Actor with ActorLogging {

  val ctx = implicitly[ActorContext]
  implicit val system = ctx.system

  val port = "/dev/ttyACM0"
  val settings = SerialSettings(
    baud = 9600,
    characterSize = 8,
    twoStopBits = false,
    parity = Parity.None
  )

  IO(Serial) ! Serial.Open(port, settings)

  var buffer = new ParsingBuffer(ByteString())
  def receive = {
    case Serial.CommandFailed(cmd: Serial.Open, reason: AccessDeniedException) =>
      println("You're not allowed to open that port!")
    case Serial.CommandFailed(cmd: Serial.Open, reason) =>
      println("Could not open port for some other reason: " + reason.getMessage)
    case Serial.Opened(settings) => {
      val operator = sender
      //do stuff with the operator, e.g. context become opened(op)
    }
    case Serial.Received(data) => {
      buffer = buffer.add(data)
      val (maybeData, newBuffer) = buffer.poll
      maybeData.foreach{s =>
        println(s"received rate: $s" )
        val rate = s.toInt * 50

        dataProducer ! DataProducer.MessagesPerSecond(rate)
      }
      buffer = newBuffer
    }
  }
}

class ParsingBuffer(data: ByteString){
  val CR = 13.toByte
  val LF = 10.toByte
  val CRLF = Array(CR,LF)

  def add(other: ByteString): ParsingBuffer = {
    new ParsingBuffer(data ++ other)
  }

  val isEmpty = data.isEmpty

  def poll:(Option[String], ParsingBuffer) = {
    val (maybeComplete, rest) = data.span(_ != CR)
    val crlf = rest.take(2).toArray
    if (crlf.deep == CRLF.deep) {
      (Some(maybeComplete.utf8String), new ParsingBuffer(rest.drop(2)))
    } else (None, this)
  }
}
