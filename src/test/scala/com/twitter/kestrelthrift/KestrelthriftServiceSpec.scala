package com.twitter.kestrelthrift

import java.nio.ByteBuffer

class KestrelthriftServiceSpec extends AbstractSpec {
  "KestrelthriftServiceServer2" should {

    val testQName = "test234"
    val kestrelif = kestrelthrift.newImpl()

    "set a key, multiget a key" in {
      kestrelif.flush(testQName)
      kestrelif.put(testQName, ByteBuffer.wrap("bluebird".getBytes))
      new String(kestrelif.get(testQName, false)().data.array) mustEqual "bluebird"
    }
    "fifo" in {
      kestrelif.flush(testQName)
      kestrelif.put(testQName, ByteBuffer.wrap("bluebird1".getBytes))
      kestrelif.put(testQName, ByteBuffer.wrap("bluebird2".getBytes))
      kestrelif.put(testQName, ByteBuffer.wrap("bluebird3".getBytes))
      new String(kestrelif.get(testQName, false)().data.array) mustEqual "bluebird1"
      new String(kestrelif.get(testQName, false)().data.array) mustEqual "bluebird2"
      new String(kestrelif.get(testQName, false)().data.array) mustEqual "bluebird3"
      require(kestrelif.get(testQName, false)() == null)
    }
    "2x set a key, multiget a key" in {
      kestrelif.flush(testQName)
      kestrelif.put(testQName, ByteBuffer.wrap("bluebird1".getBytes))
      kestrelif.put(testQName, ByteBuffer.wrap("bluebird2".getBytes))
      val values = kestrelif.multiget(testQName, 10, false)().map(i => new String(i.data.array))
      values mustEqual List("bluebird1", "bluebird2")
      require(kestrelif.get(testQName, false)() == null)
    }
    "multiput" in {
      kestrelif.flush(testQName)
      kestrelif.multiput(testQName, List(ByteBuffer.wrap("bluebird1".getBytes),
                                             ByteBuffer.wrap("bluebird2".getBytes),
                                             ByteBuffer.wrap("bluebird3".getBytes)))
      new String(kestrelif.get(testQName, false)().data.array) mustEqual "bluebird1"
      new String(kestrelif.get(testQName, false)().data.array) mustEqual "bluebird2"
      new String(kestrelif.get(testQName, false)().data.array) mustEqual "bluebird3"
      require(kestrelif.get(testQName, false)() == null)
    }
    "transaction ack/fail" in {
      kestrelif.flush(testQName)
      kestrelif.put(testQName, ByteBuffer.wrap("bluebird3".getBytes))
      var item = kestrelif.get(testQName, true)()
      new String(item.data.array) mustEqual "bluebird3"
      kestrelif.fail(testQName, Set(item.xid))

      item = kestrelif.get(testQName, true)()
      new String(item.data.array) mustEqual "bluebird3"
      kestrelif.fail(testQName, Set(item.xid))

      item = kestrelif.get(testQName, true)()
      new String(item.data.array) mustEqual "bluebird3"
      kestrelif.ack(testQName, Set(item.xid))

      item = kestrelif.get(testQName, false)()
      require(item == null)
    }
  }
}
