package com.twitter.kestrelthrift

import scala.collection.mutable.Map
import scala.collection.Set
import com.twitter.util._
import config._
import net.lag.kestrel._
import net.lag.kestrel.config._
import java.nio.ByteBuffer

class KestrelthriftServiceImpl(config: KestrelthriftServiceConfig) extends KestrelthriftServiceServer {
  val serverName = "Kestrelthrift"
  val thriftPort = config.thriftPort

  //val dataDir = "/var/spool/kestrel"
  val dataDir = "./data" // TODO: move to config
  // TODO: commit
  // TODO: batch get

  val qs = new QueueCollection(dataDir, new FakeTimer(), new QueueBuilder().apply(), List())

  def get(queueName: String, maxItems: Int, transaction: Boolean) = {
    val future = qs.remove(queueName, None, transaction)
    future.map { item =>
      item match {
        case None => List()
        case Some(item) => List(new Item(ByteBuffer.wrap(item.data), item.xid))
      }
    }
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
