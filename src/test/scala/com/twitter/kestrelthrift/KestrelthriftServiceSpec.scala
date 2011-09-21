package com.twitter.kestrelthrift

class KestrelthriftServiceSpec extends AbstractSpec {
  "KestrelthriftService" should {

    // TODO: Please implement your own tests.

    "set a key, get a key" in {
      kestrelthrift.put("name", "bluebird")()
      kestrelthrift.get("name")() mustEqual "bluebird"
      kestrelthrift.get("what?")() must throwA[Exception]
    }
  }
}
