package com.twitter.kestrelthrift

import com.twitter.ostrich.admin.RuntimeEnvironment

object Main {
  def main(args: Array[String]) {
    val env = RuntimeEnvironment(this, args)
    val service = env.loadRuntimeConfig[KestrelthriftServiceServer]
    service.start()
  }
}

