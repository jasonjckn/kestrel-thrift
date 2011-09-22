package com.twitter.kestrelthrift

import java.net.InetSocketAddress
import java.util.{List => JList, Map => JMap, Set => JSet}
import scala.collection._
import scala.collection.JavaConversions._
import org.apache.thrift.protocol._

import com.twitter.conversions.time._
import com.twitter.finagle.builder._
import com.twitter.finagle.stats._
import com.twitter.finagle.thrift._
import com.twitter.logging.Logger
import com.twitter.ostrich.admin.Service
import com.twitter.util._
import java.util.concurrent.{TimeUnit}
import org.jboss.netty.util.{HashedWheelTimer, Timeout, Timer, TimerTask}
import net.lag.kestrel._
import config._
import net.lag.kestrel.config._
import com.twitter.kestrelthrift.thrift.KestrelthriftService.ServiceIface

class KestrelthriftService3(val impl: KestrelthriftServiceImpl, val protocolFactory: TProtocolFactory) 
    extends com.twitter.kestrelthrift.thrift.KestrelthriftService.Service(impl.toThrift, protocolFactory) {
    
    override def release() {
        impl.release()
        super.release()
    }
}


class KestrelthriftServiceServer2(config: KestrelthriftServiceConfig) extends Service {
  val log = Logger.get(getClass)

  def thriftCodec = ThriftServerFramedCodec()
  val thriftProtocolFactory = new TBinaryProtocol.Factory()

  var server: Server = null

  val serverName = "Kestrelthrift"
  val thriftPort = config.thriftPort

  // this means no timeout will be at better granularity than 10ms.
  val timer = new HashedWheelTimer(10, TimeUnit.MILLISECONDS)
  val qs = new QueueCollection("<ignored>", new NettyTimer(timer), new QueueBuilder().apply(), List())
  qs.loadQueues()

  def newImpl() = {
    new KestrelthriftServiceImpl(qs)
  }

  def start = {
    val serverAddr = new InetSocketAddress(thriftPort)
    server = ServerBuilder().codec(thriftCodec)
                            .name(serverName)
                            .reportTo(new OstrichStatsReceiver)
                            .bindTo(serverAddr).build(() => {
        new KestrelthriftService3(new KestrelthriftServiceImpl(qs),thriftProtocolFactory)
    })
  }

  def shutdown = synchronized {
    if (server != null) {
      server.close(0.seconds)
    }
  }
}


// vim: set ts=4 sw=4 et:
