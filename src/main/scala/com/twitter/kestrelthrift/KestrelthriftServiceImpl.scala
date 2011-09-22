package com.twitter.kestrelthrift

import scala.collection.mutable.Map
import scala.collection.Set
import com.twitter.util._
import config._
import net.lag.kestrel._
import net.lag.kestrel.config._
import java.nio.ByteBuffer
import java.util.concurrent.{TimeUnit}
import org.jboss.netty.util.{HashedWheelTimer, Timeout, Timer, TimerTask}

class KestrelthriftServiceImpl(config: KestrelthriftServiceConfig) extends KestrelthriftServiceServer {
  val serverName = "Kestrelthrift"
  val thriftPort = config.thriftPort

  // this means no timeout will be at better granularity than 10ms.
  val timer = new HashedWheelTimer(10, TimeUnit.MILLISECONDS)
  val qs = new QueueCollection("<ignored>", new NettyTimer(timer), new QueueBuilder().apply(), List())
  qs.loadQueues()

  def get(queueName: String, transaction: Boolean) = {
    qs.remove(queueName, None, transaction).map { item =>
      item match {
        case None => null
        case Some(item) => new Item(ByteBuffer.wrap(item.data), item.xid)
      }
    }
  }

  def multiget(queueName: String, maxItems: Int, transaction: Boolean) = {
    val futureList = for(i <- 1 to maxItems) 
      yield qs.remove(queueName, None, transaction).map { item =>
            item match {
              case None => null
              case Some(item) => new Item(ByteBuffer.wrap(item.data), item.xid)
            }
          }
    val agg = Future.collect(futureList.toSeq)
    agg.map(seq => seq.filter(_ != null))
  }

  def put(queueName: String, items: Seq[ByteBuffer]) = {
    for(i <- items) 
        qs.add(queueName, i.array)
    Future.void
  }

  def ack(queueName: String, xids: Set[Int]) = {
    for(id <- xids) 
      qs.confirmRemove(queueName, id)
    Future.void
  }

  def fail(queueName: String, xids: Set[Int]) = {
    for(id <- xids) 
      qs.unremove(queueName, id)
    Future.void
  }

  def flush(queueName: String) = {
    qs.flush(queueName)
    Future.void
  }

}
