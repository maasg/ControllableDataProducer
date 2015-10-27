package com.gm.serial

import akka.util.ByteString
import org.scalatest._

import scala.collection.mutable.ArrayBuffer

class ParsingBufferSpec extends FlatSpec with Matchers {

  val CR= 13.toByte
  val LF = 10.toByte

  val CRLF = Array(CR,LF)

  "ParsingBuffer" should "not emit data from an empty buffer" in {
    val buffer  = new ParsingBuffer(ByteString())
    val (res, newBuff) = buffer.poll
    res should be (None)
    newBuff should be ('empty)
  }

  it should "append new data to an existing buffer" in {
    val empty = new ParsingBuffer(ByteString())
    val buff = empty.add(ByteString("hello"))
    empty should not be (buff)
    buff should not be ('empty)
  }

  it should "not emit data if not CRLF is found" in {
    val empty = new ParsingBuffer(ByteString())
    val buff = empty.add(ByteString("hello"))
    val (res, _) = buff.poll
    res should be (None)
  }

  it should "emit data when CRLF is found" in {
    val bytes = "aabaa".getBytes("UTF-8")
    val base = new ParsingBuffer(ByteString(bytes))
    val completed = base.add(ByteString(CRLF))
    val (res, rest) = completed.poll
    res.get should be ("aabaa")
    rest should be ('empty)
  }

  it should "preserve data after a new line" in {
    val aabaa = "aabaa".getBytes("UTF-8")
    val cdc= "cdc".getBytes("UTF-8")
    val aabaaData = new ParsingBuffer(ByteString(aabaa))
    val firstLine = aabaaData.add(ByteString(CRLF))
    val cdcData = firstLine.add(ByteString(cdc))
    val secondLine = cdcData.add(ByteString(CRLF))

    val (resAABAA, second) = secondLine.poll
    val (resCDC, empty) = second.poll
    resAABAA.get should be ("aabaa")
    resCDC.get should be ("cdc")
    empty should be ('empty)
  }

  it should "not emit data when only CR is found" in {
    val bytes = "aabaa".getBytes("UTF-8")
    val base = new ParsingBuffer(ByteString(bytes))
    val completed = base.add(ByteString(Array(CR)))
    val (res, rest) = completed.poll
    res should be (None)
  }

}
