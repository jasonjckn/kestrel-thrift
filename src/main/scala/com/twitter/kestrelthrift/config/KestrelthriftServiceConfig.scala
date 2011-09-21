package com.twitter.kestrelthrift
package config

import com.twitter.logging.Logger
import com.twitter.logging.config._
import com.twitter.ostrich.admin.{RuntimeEnvironment, ServiceTracker}
import com.twitter.ostrich.admin.config._
import com.twitter.util.Config

class KestrelthriftServiceConfig extends ServerConfig[KestrelthriftServiceServer] {
  var thriftPort: Int = 9999

  def apply(runtime: RuntimeEnvironment) = new KestrelthriftServiceImpl(this)
}
