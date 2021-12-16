package io.github.yuemenglong.redis.nio

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, SocketChannel}
import java.util

class Channel(val key: SelectionKey, nio: Nio) {
  final val BUF_SIZE = 4096
  final val BUF_COUNT = 128

  private val channel: SocketChannel = key.channel().asInstanceOf[SocketChannel]

  //noinspection ScalaUnnecessaryParentheses
  private var readFn: util.Deque[ByteBuffer] => Unit = (_ => {})
  //noinspection ScalaUnnecessaryParentheses
  private var errorFn: Throwable => Unit = (_ => {})
  private var closeFn: () => Unit = () => {}
  private var _attachment: Object = _
  private val _readBuf: util.LinkedList[ByteBuffer] = new util.LinkedList[ByteBuffer]()
  private val _writeBuf: util.LinkedList[ByteBuffer] = new util.LinkedList[ByteBuffer]()

  def onRead(fn: util.Deque[ByteBuffer] => Unit): Unit = {
    readFn = fn
  }

  def onError(fn: Throwable => Unit): Unit = {
    errorFn = fn
  }

  def onClose(fn: () => Unit): Unit = {
    closeFn = fn
  }

  private[redis] def doRead(): Unit = try {
    var cont = true
    while (cont && _readBuf.size < BUF_COUNT) {
      val buf = ByteBuffer.allocate(BUF_SIZE)
      val len = channel.read(buf)
      len match {
        case 0 => cont = false
        case n if n < 0 => throw new IOException("EOF")
        case n if n > 0 =>
          buf.flip()
          _readBuf.addLast(buf)
        //            readFn(buf)
        //          _readBuf += buf
      }
    }
    readFn(_readBuf)
    //    _readBuf.size match {
    //      case 0 =>
    //      case 1 => readFn(_readBuf(0))
    //      case _ => val bs = new ByteArrayOutputStream()
    //        _readBuf.foreach(b => bs.write(b.array(), b.position(), b.remaining()))
    //        readFn(ByteBuffer.wrap(bs.toByteArray))
    //    }
    //    _readBuf.clear()
  } catch {
    case _: IOException => doClose()
  }

  private[redis] def doWrite(expect: Int): Unit = try {
    // 立刻写优化
    if (_writeBuf.size() <= expect) {
      var cont = true
      while (cont && _writeBuf.size() > 0) {
        val buf = _writeBuf.peekFirst()
        channel.write(buf)
        if (buf.remaining() == 0) {
          _writeBuf.pollFirst()
        } else {
          cont = false
        }
      }
    }
    if (_writeBuf.size() == 0) {
      key.attach(IOAttach(this))
      key.interestOps(SelectionKey.OP_READ)
    } else {
      key.attach(IOAttach(this))
      key.interestOps(SelectionKey.OP_WRITE | SelectionKey.OP_READ)
    }
  } catch {
    case _: IOException => doClose()
  }

  def doWrite(buf: Array[Byte]): Unit = {
    doWrite(ByteBuffer.wrap(buf))
  }

  //  def doWrite(buf: ByteBuffer): Unit = try {
  //    if (buf.remaining() == 0) {
  //      return
  //    }
  //    // 立刻写优化
  //    if (_writeBuf.size() == 0) {
  //      channel.write(buf)
  //      if (buf.remaining() > 0) {
  //        _writeBuf.addLast(buf)
  //      }
  //    } else {
  //      _writeBuf.addLast(buf)
  //    }
  //    if (_writeBuf.size() == 0) {
  //      key.attach(IOAttach(this))
  //      key.interestOps(SelectionKey.OP_READ)
  //    } else {
  //      key.attach(IOAttach(this))
  //      key.interestOps(SelectionKey.OP_WRITE | SelectionKey.OP_READ)
  //    }
  //  } catch {
  //    case _: IOException => doClose()
  //  }

  def doWrite(buf: ByteBuffer): Unit = {
    _writeBuf.addLast(buf)
    doWrite(1)
  }

  def doWrite(bufs: Seq[ByteBuffer]): Unit = {
    bufs.foreach(_writeBuf.addLast)
    doWrite(bufs.size)
  }

  private def doError(e: Throwable): Unit = {
    try {
      errorFn(e)
    } finally {
      doClose()
    }
  }

  //noinspection DangerousCatchAll
  def doClose(): Unit = {
    try {
      _readBuf.clear()
      _writeBuf.clear()
      channel.close()
    } catch {
      case _: Throwable =>
    }
    key.cancel()
    closeFn()
  }

  def attach(obj: Object): Unit = _attachment = obj

  def attachment(): Object = _attachment
}
