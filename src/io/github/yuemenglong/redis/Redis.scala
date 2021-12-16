package io.github.yuemenglong.redis

import java.net.InetSocketAddress
import io.github.yuemenglong.redis.nio.Nio
import io.github.yuemenglong.redis.proto.{ProtocolExecption, QuitExecption, RedisProto, ShutdownExecption}
import io.github.yuemenglong.redis.store.RedisUStore

class Redis(init: Int = 0) {
  val nio = new Nio
  var store: RedisUStore = new RedisUStore(init)

  def start(addr: InetSocketAddress): Unit = {
    store.info.port.set(addr.getPort)
    nio.doAccept(addr, channel => {
      store.info.conn.incrementAndGet()
      channel.attach(new RedisProto(store))
      channel.onRead(req => {
        try {
          val res = channel.attachment().asInstanceOf[RedisProto].handle(req)
          channel.doWrite(res)
        } catch {
          case _: ProtocolExecption => channel.doClose()
          case _: QuitExecption => channel.doClose()
          case _: ShutdownExecption => nio.stop()
          case e: Throwable => e.printStackTrace()
            channel.doClose()
        }
      })
      channel.onClose(() => {
        store.info.conn.decrementAndGet()
      })
    })
    nio.loop()
  }

  def start(port: Int): Unit = {
    start(new InetSocketAddress(port))
  }

  def stop(): Unit = {
    nio.stop()
  }

  def running(): Boolean = {
    nio.running
  }
}

//noinspection ConvertExpressionToSAM
object Main {

  def main(args: Array[String]): Unit = {
    val port = args.find(_.startsWith("-p")).map(_.substring(2)).map(Integer.parseInt).getOrElse(6379)
    val capacity = args.find(_.startsWith("-c")).map(_.substring(2)).map(Integer.parseInt).getOrElse(0)
    val redis = new Redis(capacity)
    Thread.currentThread().setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      override def uncaughtException(t: Thread, e: Throwable): Unit = {
        println(s"ERR @${port} ${e}")
        e.printStackTrace()
      }
    })
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      override def run(): Unit = println(s"Shutdown Redis(Java) @ ${port}")
    }))
    println(s"Start Redis(Java) @ ${port}")
    redis.start(new InetSocketAddress(port))
    println(s"Stop Redis(Java) @ ${port}")
  }

}
