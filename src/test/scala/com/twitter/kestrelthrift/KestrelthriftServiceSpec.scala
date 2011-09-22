package com.twitter.kestrelthrift

import java.nio.ByteBuffer

class KestrelthriftServiceSpec extends AbstractSpec {
  "KestrelthriftService" should {

    // TODO: Please implement your own tests.

    /*
    "set a key, get a key" in {
      kestrelthrift.put("name", "bluebird")()
      kestrelthrift.get("name")() mustEqual "bluebird"
      kestrelthrift.get("what?")() must throwA[Exception]
    }
    */
    "set a key, get a key" in {
      kestrelthrift.flush("test234")
      kestrelthrift.put("test234", List(ByteBuffer.wrap("bluebird".getBytes)))
      new String(kestrelthrift.get("test234", 1, false)().head.data.array) mustEqual "bluebird"
    }
    "2x set a key, get a key" in {
      kestrelthrift.flush("test234")
      kestrelthrift.put("test234", List(ByteBuffer.wrap("bluebird1".getBytes)))
      kestrelthrift.put("test234", List(ByteBuffer.wrap("bluebird2".getBytes)))
      new String(kestrelthrift.get("test234", 1, false)().head.data.array) mustEqual "bluebird1"
      new String(kestrelthrift.get("test234", 1, false)().head.data.array) mustEqual "bluebird2"
    }
  }
}
