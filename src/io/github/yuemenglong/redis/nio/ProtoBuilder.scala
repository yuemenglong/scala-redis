package io.github.yuemenglong.redis.nio

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util
import scala.collection.JavaConversions._

class ProtoBuilder(val channel: Channel) {
  private var first: ProtoNode = _
  private var cur: ProtoNode = _
  private var buf: ByteBuffer = _

  channel.onRead(buffers => {
    merge(buffers)
    while (cur != null && buf != null
      && buf.remaining() >= cur.n) {
      val data = new Array[Byte](cur.n)
      buf.get(data)
      if (buf.remaining() == 0) {
        buf = null
      }
      if (cur.thenFn != null) {
        cur.thenFn(data)
      } else if (cur.doneFn != null) {
        cur.doneFn(data)
      }
    }
  })

  class ProtoNode {
    private[nio] var thenFn: Array[Byte] => Unit = _
    private[nio] var doneFn: Array[Byte] => Unit = _
    private[nio] var n: Int = -1

    def andThen(fn: Array[Byte] => ProtoNode): ProtoNode = {
      val ret = new ProtoNode
      thenFn = res => {
        val act = fn(res)
        ret.n = act.n
        cur = ret
      }
      ret
    }

    def done(fn: Array[Byte] => Unit): Unit = {
      doneFn = res => {
        fn(res)
        cur = first
      }
    }
  }

  private def merge(buffers: util.Deque[ByteBuffer]): Unit = {
    val bs = new ByteArrayOutputStream()
    bs.write(buf.array(), buf.position(), buf.remaining())
    buffers.foreach(b => {
      bs.write(b.array(), b.position(), b.remaining())
    })
    buf = ByteBuffer.wrap(bs.toByteArray)
  }

  def read(n: Int): ProtoNode = {
    val node = new ProtoNode
    node.n = n
    if (first == null) {
      first = node
      cur = node
    }
    node
  }
}
