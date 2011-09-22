package com.twitter.kestrelthrift

import java.nio.ByteBuffer

class KestrelthriftServiceSpec extends AbstractSpec {
  "KestrelthriftService" should {

    val testQName = "test234"

    "set a key, multiget a key" in {
      kestrelthrift.flush(testQName)
      kestrelthrift.put(testQName, ByteBuffer.wrap("bluebird".getBytes))
      new String(kestrelthrift.get(testQName, false)().data.array) mustEqual "bluebird"
    }
    "fifo" in {
      kestrelthrift.flush(testQName)
      kestrelthrift.put(testQName, ByteBuffer.wrap("bluebird1".getBytes))
      kestrelthrift.put(testQName, ByteBuffer.wrap("bluebird2".getBytes))
      kestrelthrift.put(testQName, ByteBuffer.wrap("bluebird3".getBytes))
      new String(kestrelthrift.get(testQName, false)().data.array) mustEqual "bluebird1"
      new String(kestrelthrift.get(testQName, false)().data.array) mustEqual "bluebird2"
      new String(kestrelthrift.get(testQName, false)().data.array) mustEqual "bluebird3"
      require(kestrelthrift.get(testQName, false)() == null)
    }
    "2x set a key, multiget a key" in {
      kestrelthrift.flush(testQName)
      kestrelthrift.put(testQName, ByteBuffer.wrap("bluebird1".getBytes))
      kestrelthrift.put(testQName, ByteBuffer.wrap("bluebird2".getBytes))
      val values = kestrelthrift.multiget(testQName, 10, false)().map(i => new String(i.data.array))
      values mustEqual List("bluebird1", "bluebird2")
      require(kestrelthrift.get(testQName, false)() == null)
    }
    "multiput" in {
      kestrelthrift.flush(testQName)
      kestrelthrift.multiput(testQName, List(ByteBuffer.wrap("bluebird1".getBytes),
                                             ByteBuffer.wrap("bluebird2".getBytes),
                                             ByteBuffer.wrap("bluebird3".getBytes)))
      new String(kestrelthrift.get(testQName, false)().data.array) mustEqual "bluebird1"
      new String(kestrelthrift.get(testQName, false)().data.array) mustEqual "bluebird2"
      new String(kestrelthrift.get(testQName, false)().data.array) mustEqual "bluebird3"
      require(kestrelthrift.get(testQName, false)() == null)
    }
    "transaction ack/fail" in {
      kestrelthrift.flush(testQName)
      kestrelthrift.put(testQName, ByteBuffer.wrap("bluebird3".getBytes))
      var item = kestrelthrift.get(testQName, true)()
      new String(item.data.array) mustEqual "bluebird3"
      kestrelthrift.fail(testQName, Set(item.xid))

      item = kestrelthrift.get(testQName, true)()
      new String(item.data.array) mustEqual "bluebird3"
      kestrelthrift.fail(testQName, Set(item.xid))

      item = kestrelthrift.get(testQName, true)()
      new String(item.data.array) mustEqual "bluebird3"
      kestrelthrift.ack(testQName, Set(item.xid))

      item = kestrelthrift.get(testQName, false)()
      require(item == null)
    }
  }
}
