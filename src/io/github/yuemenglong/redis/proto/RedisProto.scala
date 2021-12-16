package io.github.yuemenglong.redis.proto

import java.nio.ByteBuffer
import java.util
import io.github.yuemenglong.redis.store.RedisUStore

class ProtocolExecption extends RuntimeException("Invalid Protocol")

class CommandException extends RuntimeException

class QuitExecption extends CommandException()

class ShutdownExecption extends CommandException()

trait Reply {
  //noinspection AccessorLikeMethodIsUnit
  def toByteBuffer: Seq[ByteBuffer]
}

case class SimpleStringReply(status: String = "OK") extends Reply {
  def toByteBuffer: Seq[ByteBuffer] = {
    Seq(ByteBuffer.wrap(s"+${status}\r\n".getBytes()))
  }
}

case class ErrorReply(errType: String = "ERR", errMsg: String) extends Reply {
  def toByteBuffer: Seq[ByteBuffer] = {
    Seq(ByteBuffer.wrap(s"-${errType} ${errMsg}\r\n".getBytes()))
  }
}

case class IntegerReply(value: Long) extends Reply {
  def toByteBuffer: Seq[ByteBuffer] = {
    Seq(ByteBuffer.wrap(s":${value}\r\n".getBytes()))
  }
}

case class BulkStringReply(data: Array[Byte]) extends Reply {
  def toByteBuffer: Seq[ByteBuffer] = {
    data match {
      case null => Seq(ByteBuffer.wrap("$-1\r\n".getBytes()))
      case _ =>
        Seq(
          ByteBuffer.wrap(s"$$${data.length}\r\n".getBytes()),
          ByteBuffer.wrap(data),
          ByteBuffer.wrap("\r\n".getBytes())
        )
    }
  }
}

case class ArrayReply(data: Seq[_ <: Reply]) extends Reply {
  def toByteBuffer: Seq[ByteBuffer] = {
    data match {
      case null => Seq(ByteBuffer.wrap("*-1\r\n".getBytes()))
      case _ =>
        Seq(ByteBuffer.wrap(s"*${data.length}\r\n".getBytes())) ++ data.flatMap(_.toByteBuffer)
    }
  }
}

class RedisProto(store: RedisUStore) {

  val reader: RedisProtoReader = new RedisProtoReader
  val handlers: Map[String, Array[Array[Byte]] => Reply] = Map(
    "COMMAND" -> (_ => command()),
    "SET" -> (args => set(args(1), args(2))),
    "SETNX" -> (args => setnx(args(1), args(2))),
    "GET" -> (args => get(args(1))),
    "DEL" -> (args => del(args(1))),
    "SADD" -> (args => sadd(args(1), args(2))),
    "SPOP" -> (args => spop(args(1))),
    "SMEMBERS" -> (args => smembers(args(1))),
    "SCARD" -> (args => scard(args(1))),
    "SREM" -> (args => srem(args(1), args(2))),
    "HSET" -> (args => hset(args(1), args(2), args(3))),
    "HGET" -> (args => hget(args(1), args(2))),
    "HDEL" -> (args => hdel(args(1), args(2))),
    "HGETALL" -> (args => hgetAll(args(1))),
    "LPUSH" -> (args => lpush(args(1), args(2))),
    "RPUSH" -> (args => rpush(args(1), args(2))),
    "LPOP" -> (args => lpop(args(1))),
    "RPOP" -> (args => rpop(args(1))),
    "LLEN" -> (args => llen(args(1))),
    "INCR" -> (args => incr(args(1))),
    "INCRBY" -> (args => incrBy(args(1), args(2))),
    "DECR" -> (args => decr(args(1))),
    "DECRBY" -> (args => decrBy(args(1), args(2))),
    "KEYS" -> (args => keys(args(1))),
    "INFO" -> (_ => info()),
    "AUTH" -> (_ => auth()),
    "FLUSHALL" -> (_ => flushAll()),
    "PING" -> (_ => ping()),
    "SHUTDOWN" -> (_ => shutdown()),
    "QUIT" -> (_ => quit())
  )

  def errorString(e: Throwable, pre: String = ""): String = {
    val es = Array(e.toString, pre).filter(_.nonEmpty).mkString(", ")
    if (e.getCause != null && e.getCause != e) {
      errorString(e.getCause, es)
    } else {
      s"[${es}]__" + e.getStackTrace.mkString("__")
    }
  }

  def handleArgs(args: Array[Array[Byte]]): Reply = {
    try {
      val cmd = new String(args(0)).toUpperCase()
      val handler = handlers.getOrElse(cmd, null)
      handler match {
        case null => throw new RuntimeException(s"Unsupported Command: ${cmd}")
        case _ => handler(args)
      }
      //      cmd match {
      //        case "COMMAND" => command()
      //        case "SET" => set(new String(args(1)), args(2))
      //        case "SETNX" => setnx(new String(args(1)), args(2))
      //        case "GET" => get(new String(args(1)))
      //        case "DEL" => del(new String(args(1)))
      //        case "SADD" => sadd(new String(args(1)), args(2))
      //        case "SPOP" => spop(new String(args(1)))
      //        case "SMEMBERS" => smembers(new String(args(1)))
      //        case "SCARD" => scard(new String(args(1)))
      //        case "SREM" => srem(new String(args(1)), args(2))
      //        case "HSET" => hset(new String(args(1)), new String(args(2)), args(3))
      //        case "HGET" => hget(new String(args(1)), new String(args(2)))
      //        case "HDEL" => hdel(new String(args(1)), new String(args(2)))
      //        case "HGETALL" => hgetAll(new String(args(1)))
      //        case "LPUSH" => lpush(new String(args(1)), args(2))
      //        case "RPUSH" => rpush(new String(args(1)), args(2))
      //        case "LPOP" => lpop(new String(args(1)))
      //        case "RPOP" => rpop(new String(args(1)))
      //        case "LLEN" => llen(new String(args(1)))
      //        case "INCR" => incrBy(new String(args(1)), "1".getBytes())
      //        case "INCRBY" => incrBy(new String(args(1)), args(2))
      //        case "DECR" => decrBy(new String(args(1)), "1".getBytes())
      //        case "DECRBY" => decrBy(new String(args(1)), args(2))
      //        case "KEYS" => keys(new String(args(1)))
      //        case "INFO" => info()
      //        case "AUTH" => auth()
      //        case "FLUSHALL" => flushAll()
      //        case "PING" => ping()
      //        case "SHUTDOWN" => shutdown()
      //        case "QUIT" => throw new QuitExecption
      //        case _ => throw new RuntimeException(s"Unsupported Command: ${cmd}")
      //      }
    } catch {
      case e: CommandException => throw e
      case e: Throwable => ErrorReply(errMsg = errorString(e))
    }
  }

  def handle(buffers: util.Deque[ByteBuffer]): Seq[ByteBuffer] = {
    //    val bs = new ByteArrayOutputStream()
    reader.read(buffers).map(handleArgs).flatMap(_.toByteBuffer)
    //      .foreach(_.getBytes(bs))
    //    ByteBuffer.wrap(bs.toByteArray)
  }

  def command(): ArrayReply = {
    ArrayReply(null)
  }

  def set(key: Array[Byte], value: Array[Byte]): SimpleStringReply = {
    store.set(key, value)
    SimpleStringReply()
  }

  def setnx(key: Array[Byte], value: Array[Byte]): IntegerReply = {
    val res = store.setnx(key, value)
    IntegerReply(res)
  }

  def get(key: Array[Byte]): BulkStringReply = {
    val data = store.get(key)
    BulkStringReply(data)
  }

  def del(key: Array[Byte]): IntegerReply = {
    val ret = store.del(key)
    IntegerReply(ret)
  }

  def sadd(key: Array[Byte], value: Array[Byte]): IntegerReply = {
    val ret = store.sadd(key, value)
    IntegerReply(ret)
  }

  def spop(key: Array[Byte]): BulkStringReply = {
    val ret = store.spop(key)
    BulkStringReply(ret)
  }

  def smembers(key: Array[Byte]): ArrayReply = {
    val bulks = store.smember(key).map(BulkStringReply)
    ArrayReply(bulks)
  }

  def scard(key: Array[Byte]): IntegerReply = {
    val size = store.scard(key)
    IntegerReply(size)
  }

  def srem(key: Array[Byte], value: Array[Byte]): IntegerReply = {
    val ret = store.srem(key, value)
    IntegerReply(ret)
  }

  def hset(key: Array[Byte], hkey: Array[Byte], value: Array[Byte]): IntegerReply = {
    val ret = store.hset(key, hkey, value)
    IntegerReply(ret)
  }

  def hget(key: Array[Byte], hkey: Array[Byte]): BulkStringReply = {
    val ret = store.hget(key, hkey)
    BulkStringReply(ret)
  }

  def hdel(key: Array[Byte], hkey: Array[Byte]): IntegerReply = {
    val ret = store.hdel(key, hkey)
    IntegerReply(ret)
  }

  def hgetAll(key: Array[Byte]): ArrayReply = {
    val ret = store.hgetAll(key).map(BulkStringReply)
    ArrayReply(ret)
  }

  def lpush(key: Array[Byte], value: Array[Byte]): IntegerReply = {
    val ret = store.lpush(key, value)
    IntegerReply(ret)
  }

  def rpush(key: Array[Byte], value: Array[Byte]): IntegerReply = {
    val ret = store.rpush(key, value)
    IntegerReply(ret)
  }

  def lpop(key: Array[Byte]): BulkStringReply = {
    val ret = store.lpop(key)
    BulkStringReply(ret)
  }

  def rpop(key: Array[Byte]): BulkStringReply = {
    val ret = store.rpop(key)
    BulkStringReply(ret)
  }

  def llen(key: Array[Byte]): IntegerReply = {
    val ret = store.llen(key)
    IntegerReply(ret)
  }


  def incr(key: Array[Byte]): IntegerReply = {
    val ret = store.incr(key)
    IntegerReply(ret)
  }

  def incrBy(key: Array[Byte], value: Array[Byte]): IntegerReply = {
    val ret = store.incrBy(key, value)
    IntegerReply(ret)
  }

  def decr(key: Array[Byte]): IntegerReply = {
    val ret = store.decr(key)
    IntegerReply(ret)
  }

  def decrBy(key: Array[Byte], value: Array[Byte]): IntegerReply = {
    val ret = store.decrBy(key, value)
    IntegerReply(ret)
  }

  def keys(pat: Array[Byte]): ArrayReply = {
    val res = store.keys(pat).map(k => BulkStringReply(k.getBytes()))
    ArrayReply(res)
  }

  def ping(): SimpleStringReply = {
    SimpleStringReply("PONG")
  }

  def info(): BulkStringReply = {
    BulkStringReply(store.info.updateAndGet().getBytes())
  }

  def auth(): SimpleStringReply = {
    SimpleStringReply()
  }

  def flushAll(): SimpleStringReply = {
    store.flushAll()
    SimpleStringReply()
  }

  def shutdown(): Reply = {
    throw new ShutdownExecption
  }

  def quit(): Reply = {
    throw new QuitExecption
  }
}
