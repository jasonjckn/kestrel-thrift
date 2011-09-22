package com.twitter.kestrelthrift

import scala.collection.mutable
import scala.collection.Set
import com.twitter.util._
import config._
import net.lag.kestrel._
import net.lag.kestrel.config._
import java.nio.ByteBuffer
import java.util.concurrent.{TimeUnit}
import org.jboss.netty.util.{HashedWheelTimer, Timeout, Timer, TimerTask}
import com.twitter.finagle.{Service}



class KestrelthriftServiceImpl(queues: QueueCollection) extends KestrelthriftService{
  object pendingRATransactions {  // pending Random Access Transactions.
                                  // used for syn, ack, fail
    private val transactions = new mutable.HashMap[String, mutable.HashSet[Int]] {
      override def default(key: String) = {
        val rv = new mutable.HashSet[Int]
        this(key) = rv
        rv
      }
    }

    def remove(name: String, xid: Int): Boolean = synchronized {
      transactions(name).remove(xid)
    }

    def add(name: String, xid: Int) = synchronized {
      transactions(name) += xid
    }

    def size(name: String): Int = synchronized { transactions(name).size }

    def cancelAll() {
      println("cancelAll")
      synchronized {
        transactions.foreach { case (name, xids) =>
          xids.foreach { xid => queues.unremove(name, xid) }
        }
        transactions.clear()
      }
    }
  }

  println("New Session")

  def release() {
    pendingRATransactions.cancelAll()
    println("Session Ended")
  }

  def get(key: String, transaction: Boolean) = {
    queues.remove(key, None, transaction).map { item =>
      item match {
        case None => null
        case Some(item) => {
          pendingRATransactions.add(key, item.xid)
          new Item(ByteBuffer.wrap(item.data), item.xid)
        }
      }
    }
  }

  def multiget(key: String, maxItems: Int, transaction: Boolean) = {
    val futureList = for(i <- 1 to maxItems) 
      yield get(key, transaction)
    val agg = Future.collect(futureList.toSeq)
    agg.map(seq => seq.filter(_ != null))
  }

  def put(key: String, item: ByteBuffer) = {
    queues.add(key, item.array)
    Future.void
  }

  def multiput(key: String, items: Seq[ByteBuffer]) = {
    for(i <- items) 
        queues.add(key, i.array)
    Future.void
  }

  def ack(key: String, xids: Set[Int]) = {
    for(xid <- xids) {
      pendingRATransactions.remove(key, xid)
      queues.confirmRemove(key, xid)
    }
    Future.void
  }

  def fail(key: String, xids: Set[Int]) = {
    for(xid <- xids) {
      pendingRATransactions.remove(key, xid)
      queues.unremove(key, xid)
    }
    Future.void
  }

  def flush(key: String) = {
    queues.flush(key)
    Future.void
  }

}
