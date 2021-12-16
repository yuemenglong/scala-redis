package io.github.yuemenglong.redis.nio

import java.net.InetSocketAddress
import java.nio.channels.{SelectionKey, Selector, ServerSocketChannel, SocketChannel}

case class AcceptAttach(fn: Channel => Unit)

case class ConnAttach(channel: Channel, fn: Channel => Unit)

case class IOAttach(channel: Channel)

//noinspection ConvertExpressionToSAM
class Nio {
  val selector: Selector = Selector.open()
  var running = false
  val events = new java.util.TreeMap[Long, () => Unit]()

  def stop(): Unit = {
    running = false
  }

  private def netEventLoop(): Unit = {
    val keys = selector.selectedKeys()
    val iter = keys.iterator()
    while (iter.hasNext) {
      val key = iter.next()
      iter.remove()
      if (key.isAcceptable) {
        val AcceptAttach(fn) = key.attachment()
        val server = key.channel().asInstanceOf[ServerSocketChannel]
        val channel = server.accept()
        channel.configureBlocking(false)
        val newKey = channel.register(selector, SelectionKey.OP_READ)
        val ch = new Channel(newKey, this)
        newKey.attach(IOAttach(ch))
        fn(ch)
      } else if (key.isConnectable) {
        val ConnAttach(ch, fn) = key.attachment()
        val channel = key.channel.asInstanceOf[SocketChannel]
        if (channel.isConnectionPending) {
          channel.finishConnect
        }
        key.attach(IOAttach(ch))
        key.interestOps(SelectionKey.OP_READ)
        fn(ch)
      } else if (key.isReadable) {
        val IOAttach(ch) = key.attachment()
        ch.doRead()
      }
      else if (key.isWritable) {
        val IOAttach(ch) = key.attachment()
        ch.doWrite(Int.MaxValue)
      }
    }
  }

  private def timerEventLoop(): Unit = {
    if (events.isEmpty) {
      return
    }
    val now = System.currentTimeMillis()
    val iter = events.entrySet().iterator()
    val fns = Stream.continually({
      iter.hasNext match {
        case true => iter.next() match {
          case e if e.getKey <= now =>
            iter.remove()
            e.getValue
          case _ => null
        }
        case false => null
      }
    }).takeWhile(_ != null).toArray
    fns.foreach(_ ())
  }

  def loop(): Unit = {
    running = true
    while (running) {
      selector.select(1)
      netEventLoop()
      timerEventLoop()
    }
    selector.close()
  }

  def doAccept(addr: InetSocketAddress, fn: Channel => Unit): Unit = {
    val server = ServerSocketChannel.open()
    server.configureBlocking(false)
    server.bind(addr)
    val key = server.register(selector, SelectionKey.OP_ACCEPT)
    key.attach(AcceptAttach(fn))
  }

  def doConnect(addr: InetSocketAddress, fn: Channel => Unit): Unit = {
    val channel = SocketChannel.open()
    channel.configureBlocking(false)
    channel.connect(addr)
    val key = channel.register(selector, SelectionKey.OP_CONNECT)
    val ch = new Channel(key, this)
    key.attach(ConnAttach(ch, fn))
  }

  def doTimeout(timeout: Int, fn: () => Unit): Long = {
    val now = System.currentTimeMillis()
    var t = 0
    while (true) {
      val key = now + timeout + t
      if (!events.containsKey(key)) {
        events.put(key, fn)
        return key
      }
      t += 1
    }
    throw new RuntimeException("Unreachable")
  }

  def doInterval(timeout: Int, fn: () => Unit): Long = {
    doTimeout(timeout, () => {
      fn()
      doInterval(timeout, fn)
    })
  }

  def clearTimeout(id: Long): Boolean = {
    events.remove(id) != null
  }

  def clearInterval(id: Long): Boolean = {
    events.remove(id) != null
  }
}
