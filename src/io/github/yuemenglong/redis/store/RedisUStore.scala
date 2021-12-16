package io.github.yuemenglong.redis.store

import java.util.regex.Pattern
import io.github.yuemenglong.redis.unsafe.{UBytes, UMap, UQueue, USet}
import io.github.yuemenglong.redis.unsafe.base.{UObj, Unsafe}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class RedisUStore(val init: Int = 0) {
  private[redis] val U: Unsafe = new Unsafe
  private[redis] var map: UMap[UBytes, UObj] = init match {
    case 0 => new UMap[UBytes, UObj](U)
    case _ => new UMap[UBytes, UObj](U, init)
  }

  val info: RedisInfo = new RedisInfo(this)

  def set(key: Array[Byte], value: Array[Byte]): Unit = {
    val k = new UBytes(U, key)
    val v = new UBytes(U, value)
    val ov = map.put(k, v)
    ov match {
      case null =>
      case _ =>
        k.freeDeep()
        ov.freeDeep()
    }
  }

  def setnx(key: Array[Byte], value: Array[Byte]): Int = {
    val k = new UBytes(U, key)
    val v = new UBytes(U, value)
    map.containsKey(k) match {
      case true =>
        k.freeDeep()
        v.freeDeep()
        0
      case false =>
        map.put(k, v)
        1
    }
  }

  def get(key: Array[Byte]): Array[Byte] = {
    val k = new UBytes(U, key)
    val v = map.get(k)
    k.freeDeep()
    v match {
      case null => null
      case ub: UBytes => ub.getBytes
      case x => throw WrongTypeException("get", key, x)
    }
  }

  def del(key: Array[Byte]): Int = {
    val k = new UBytes(U, key)
    val ov = map.remove(k)
    k.freeDeep()
    ov match {
      case null =>
        0
      case _ =>
        ov.freeDeep()
        1
    }
  }

  def sadd(key: Array[Byte], value: Array[Byte]): Int = {
    val k = new UBytes(U, key)
    val v = new UBytes(U, value)
    map.get(k) match {
      case null =>
        val set = new USet[UBytes](U)
        map.put(k, set)
        set.add(v)
        1
      case set: USet[UBytes] =>
        k.freeDeep()
        set.add(v) match {
          case true =>
            1
          case false =>
            v.freeDeep()
            0
        }
      case x =>
        k.freeDeep()
        v.freeDeep()
        throw WrongTypeException("sadd", key, x)
    }
  }

  def spop(key: Array[Byte]): Array[Byte] = {
    val k = new UBytes(U, key)
    map.get(k) match {
      case null =>
        k.freeDeep()
        null
      case set: USet[UBytes] =>
        val iter = set.iterator()
        iter.hasNext
        val v = iter.next()
        iter.remove()
        if (set.isEmpty) {
          map.remove(k)
          set.freeDeep()
        }
        val ret = v.getBytes
        k.freeDeep()
        v.freeDeep()
        ret
      case x =>
        k.freeDeep()
        throw WrongTypeException("spop", key, x)
    }
  }

  def smember(key: Array[Byte]): Array[Array[Byte]] = {
    val k = new UBytes(U, key)
    val s = map.get(k)
    k.freeDeep()
    s match {
      case null => Array()
      case set: USet[UBytes] => set.map(_.getBytes).toArray
      case x => throw WrongTypeException("smember", key, x)
    }
  }

  def scard(key: Array[Byte]): Long = {
    val k = new UBytes(U, key)
    val s = map.get(k)
    k.freeDeep()
    s match {
      case null => 0
      case set: USet[UBytes] => set.size()
      case x => throw WrongTypeException("scard", key, x)
    }
  }

  def srem(key: Array[Byte], value: Array[Byte]): Int = {
    val k = new UBytes(U, key)
    map.get(k) match {
      case null =>
        k.freeDeep()
        0
      case set: USet[UBytes] =>
        val v = new UBytes(U, value)
        val ret = set.remove(v) match {
          case true => 1
          case false => 0
        }
        if (set.size() == 0) {
          map.remove(k)
          set.freeDeep()
        }
        k.freeDeep()
        v.freeDeep()
        ret
      case x =>
        k.freeDeep()
        throw WrongTypeException("srem", key, x)
    }
  }

  def hset(key: Array[Byte], hkey: Array[Byte], value: Array[Byte]): Int = {
    val k = new UBytes(U, key)
    map.get(k) match {
      case null =>
        val hmap = new UMap[UBytes, UBytes](U)
        map.put(k, hmap)
        val hk = new UBytes(U, hkey)
        val v = new UBytes(U, value)
        hmap.put(hk, v)
        1
      case hmap: UMap[UBytes, UBytes] =>
        k.freeDeep()
        val hk = new UBytes(U, hkey)
        val v = new UBytes(U, value)
        hmap.put(hk, v) match {
          case null =>
            1
          case ov =>
            hk.freeDeep()
            ov.freeDeep()
            0
        }
      case x =>
        k.freeDeep()
        throw WrongTypeException("hset", key, x)
    }
  }

  def hget(key: Array[Byte], hkey: Array[Byte]): Array[Byte] = {
    val k = new UBytes(U, key)
    map.get(k) match {
      // 没有返回null
      case null =>
        k.freeDeep()
        null
      // 是map就返回
      case hmap: UMap[UBytes, UBytes] =>
        k.freeDeep()
        val hk = new UBytes(U, hkey)
        hmap.get(hk) match {
          case null =>
            hk.freeDeep()
            null
          case ub: UBytes =>
            hk.freeDeep()
            ub.getBytes
        }
      // 其他情况抛出异常
      case x => throw WrongTypeException("hget", key, x)
    }
  }

  def hdel(key: Array[Byte], hkey: Array[Byte]): Int = {
    val k = new UBytes(U, key)
    map.get(k) match {
      case null =>
        k.freeDeep()
        0
      case hmap: UMap[UBytes, UBytes] =>
        val hk = new UBytes(U, hkey)
        hmap.remove(hk) match {
          case null =>
            k.freeDeep()
            hk.freeDeep()
            0
          case ov =>
            if (hmap.isEmpty) {
              map.remove(k)
              hmap.freeDeep()
            }
            k.freeDeep()
            hk.freeDeep()
            ov.freeDeep()
            1
        }
      case x =>
        k.freeDeep()
        throw WrongTypeException("hdel", key, x)
    }
  }

  def hgetAll(key: Array[Byte]): Array[Array[Byte]] = {
    val k = new UBytes(U, key)
    map.get(k) match {
      case null =>
        k.freeDeep()
        Array()
      case hmap: UMap[UBytes, UBytes] =>
        k.freeDeep()
        hmap.toMap.flatMap { case (uk, uv) => Array(uk.getBytes, uv.getBytes) }.toArray
      case x =>
        k.freeDeep()
        throw WrongTypeException("hgetAll", key, x)
    }
  }

  def lpush(key: Array[Byte], value: Array[Byte]): Int = {
    val k = new UBytes(U, key)
    map.get(k) match {
      case null =>
        val queue = new UQueue[UBytes](U)
        map.put(k, queue)
        queue.addFirst(new UBytes(U, value))
        queue.size()
      case queue: UQueue[UBytes] =>
        k.freeDeep()
        queue.addFirst(new UBytes(U, value))
        queue.size()
      case x =>
        k.freeDeep()
        throw WrongTypeException("lpush", key, x)
    }
  }

  def rpush(key: Array[Byte], value: Array[Byte]): Int = {
    val k = new UBytes(U, key)
    map.get(k) match {
      case null =>
        val queue = new UQueue[UBytes](U)
        map.put(k, queue)
        queue.addLast(new UBytes(U, value))
        queue.size()
      case queue: UQueue[UBytes] =>
        k.freeDeep()
        queue.addLast(new UBytes(U, value))
        queue.size()
      case x =>
        k.freeDeep()
        throw WrongTypeException("rpush", key, x)
    }
  }

  def lpop(key: Array[Byte]): Array[Byte] = {
    val k = new UBytes(U, key)
    map.get(k) match {
      case null =>
        k.freeDeep()
        null
      case queue: UQueue[UBytes] =>
        queue.pollFirst() match {
          case null =>
            k.freeDeep()
            null
          case ub: UBytes =>
            val ret = ub.getBytes
            if (queue.size() == 0) {
              map.remove(k)
              queue.freeDeep()
            }
            ub.freeDeep()
            k.freeDeep()
            ret
        }
      case x =>
        k.freeDeep()
        throw WrongTypeException("lpop", key, x)
    }
  }

  def rpop(key: Array[Byte]): Array[Byte] = {
    val k = new UBytes(U, key)
    map.get(k) match {
      case null =>
        k.freeDeep()
        null
      case queue: UQueue[UBytes] =>
        queue.pollLast() match {
          case null =>
            k.freeDeep()
            null
          case ub: UBytes =>
            val ret = ub.getBytes
            if (queue.size() == 0) {
              map.remove(k)
              queue.freeDeep()
            }
            ub.freeDeep()
            k.freeDeep()
            ret
        }
      case x =>
        k.freeDeep()
        throw WrongTypeException("rpop", key, x)
    }
  }

  def llen(key: Array[Byte]): Int = {
    val k = new UBytes(U, key)
    map.get(k) match {
      case null =>
        k.freeDeep()
        0
      case queue: UQueue[UBytes] =>
        k.freeDeep()
        queue.size()
      case x =>
        k.freeDeep()
        throw WrongTypeException("llen", key, x)
    }
  }

  def atomic(key: Array[Byte], value: Long): Long = {
    val k = new UBytes(U, key)
    map.get(k) match {
      case null =>
        val v = new UBytes(U, String.valueOf(value).getBytes())
        map.put(k, v)
        value
      case ov: UBytes =>
        val n = new String(ov.getBytes).toLong + value
        val v = new UBytes(U, String.valueOf(n).getBytes())
        map.put(k, v)
        k.freeDeep()
        ov.freeDeep()
        n
      case x =>
        k.freeDeep()
        throw WrongTypeException("atomic", key, x)
    }
  }

  def incr(key: Array[Byte]): Long = {
    atomic(key, 1)
  }

  def incrBy(key: Array[Byte], value: Array[Byte]): Long = {
    atomic(key, new String(value).toLong)
  }

  def decr(key: Array[Byte]): Long = {
    atomic(key, -1)
  }

  def decrBy(key: Array[Byte], value: Array[Byte]): Long = {
    atomic(key, -new String(value).toLong)
  }

  def keys(pat: Array[Byte]): Seq[String] = {
    val re = Pattern.compile(new String(pat).replaceAll("\\*", ".*"))
    val iter = map.keysIterator
    val ab = new ArrayBuffer[String]()
    while (iter.hasNext) {
      val key = new String(iter.next().getBytes)
      if (re.matcher(key).matches()) {
        ab += key
      }
    }
    ab
  }

  def flushAll(): Unit = {
    if (map.size() == 0) {
      return
    }
    map.freeDeep()
    map = init match {
      case 0 => new UMap[UBytes, UObj](U)
      case _ => new UMap[UBytes, UObj](U, init)
    }
  }

  def free(): Unit = {
    map.freeDeep()
  }

  def infoKeys(): Long = map.size()

  def infoMemory(): Long = U.bytes()

  def infoCapacity(): Long = map.capacity()
}

object RedisUStore {

  def assert(boolean: Boolean): Unit = {
    if (!boolean) {
      throw new RuntimeException("Check Fail")
    }
  }

  def unitTest(): Unit = {
    val store = new RedisUStore
    assert(store.U.count() == 3)
    assert(store.U.bytes() == 552)

    {
      // map set slot node entry key value 7个
      store.set("1".getBytes(), "1".getBytes())
      assert(store.infoKeys() == 1)
      assert(store.U.count() == 7)
      assert(new String(store.get("1".getBytes())) == "1")
      store.set("1".getBytes(), "2".getBytes())
      assert(store.infoKeys() == 1)
      assert(store.U.count() == 7)
      assert(new String(store.get("1".getBytes())) == "2")
      assert(store.U.count() == 7)
      assert(store.get("2".getBytes()) == null)
      assert(store.U.count() == 7)
      assert(store.del("1".getBytes()) == 1)
      assert(store.U.count() == 3)
      assert(store.del("1".getBytes()) == 0)
      assert(store.infoKeys() == 0)
      assert(store.U.count() == 3)
      assert(store.U.bytes() == 552)
    }

    {
      // map set slot node entry key
      // set slot node value 10个
      assert(store.sadd("1".getBytes(), "1".getBytes()) == 1)
      assert(store.U.count() == 10)
      assert(store.sadd("1".getBytes(), "1".getBytes()) == 0)
      assert(store.U.count() == 10)
      assert(store.sadd("1".getBytes(), "2".getBytes()) == 1)
      assert(store.U.count() == 12)
      assert(store.srem("1".getBytes(), "1".getBytes()) == 1)
      assert(store.U.count() == 10)
      assert(store.srem("1".getBytes(), "1".getBytes()) == 0)
      assert(store.U.count() == 10)
      assert(store.scard("1".getBytes()) == 1)
      assert(store.U.count() == 10)
      assert(new String(store.spop("1".getBytes())) == "2")
      assert(store.U.count() == 3)
      assert(store.scard("1".getBytes()) == 0)
      assert(store.U.count() == 3)
      assert(store.spop("1".getBytes()) == null)
      assert(store.U.count() == 3)
      assert(store.sadd("1".getBytes(), "1".getBytes()) == 1)
      assert(store.U.count() == 10)
      assert(store.sadd("1".getBytes(), "2".getBytes()) == 1)
      assert(store.U.count() == 12)
      assert(store.del("1".getBytes()) == 1)
      assert(store.U.count() == 3)
      assert(store.del("1".getBytes()) == 0)
      assert(store.U.count() == 3)
      assert(store.U.bytes() == 552)
    }

    {
      // map set slot node entry key queue node value = 9
      assert(store.lpush("1".getBytes(), "1".getBytes()) == 1)
      assert(store.U.count() == 9)
      assert(store.lpush("1".getBytes(), "2".getBytes()) == 2)
      assert(store.U.count() == 11)
      assert(store.rpush("1".getBytes(), "3".getBytes()) == 3)
      assert(store.U.count() == 13)
      assert(store.llen("1".getBytes()) == 3)
      assert(store.U.count() == 13)
      assert(new String(store.lpop("1".getBytes())) == "2")
      assert(store.U.count() == 11)
      assert(store.llen("1".getBytes()) == 2)
      assert(store.U.count() == 11)
      assert(new String(store.rpop("1".getBytes())) == "3")
      assert(store.U.count() == 9)
      assert(new String(store.rpop("1".getBytes())) == "1")
      assert(store.U.count() == 3)
      assert(store.llen("1".getBytes()) == 0)
      assert(store.U.count() == 3)
      assert(store.rpop("1".getBytes()) == null)
      assert(store.U.count() == 3)
      assert(store.rpop("2".getBytes()) == null)
      assert(store.U.count() == 3)
      assert(store.rpush("1".getBytes(), "1".getBytes()) == 1)
      assert(store.U.count() == 9)
      assert(store.rpush("1".getBytes(), "2".getBytes()) == 2)
      assert(store.U.count() == 11)
      assert(store.lpush("1".getBytes(), "3".getBytes()) == 3)
      assert(store.U.count() == 13)
      assert(store.del("1".getBytes()) == 1)
      assert(store.U.count() == 3)
      assert(store.del("1".getBytes()) == 0)
      assert(store.U.count() == 3)
      assert(store.U.bytes() == 552)
    }

    {
      // map set slot node entry key value
      assert(store.incrBy("1".getBytes(), "1".getBytes()) == 1)
      assert(store.U.count() == 7)
      assert(store.incrBy("1".getBytes(), "10".getBytes()) == 11)
      assert(store.U.count() == 7)
      assert(store.decrBy("1".getBytes(), "1".getBytes()) == 10)
      assert(store.U.count() == 7)
      assert(store.del("1".getBytes()) == 1)
      assert(store.U.count() == 3)
      assert(store.U.bytes() == 552)
    }

    {
      store.set("a".getBytes(), "1".getBytes())
      store.set("b".getBytes(), "2".getBytes())
      store.sadd("c".getBytes(), "1".getBytes())
      store.sadd("c".getBytes(), "2".getBytes())
      store.sadd("d".getBytes(), "1".getBytes())
      store.sadd("d".getBytes(), "1".getBytes())
      store.lpush("e".getBytes(), "1".getBytes())
      store.lpush("e".getBytes(), "2".getBytes())
      store.lpush("e".getBytes(), "3".getBytes())
      store.rpush("e".getBytes(), "4".getBytes())
      store.rpush("e".getBytes(), "5".getBytes())
      store.rpush("e".getBytes(), "6".getBytes())
      store.flushAll()
      assert(store.U.count() == 3)
      assert(store.U.bytes() == 552)
    }
  }

  def performanceTest(): Unit = {
    // 读 写 删 各10w次
    val store = new RedisUStore
    println(store.U.count())
    println(store.U.bytes())
    val start = System.currentTimeMillis()
    (1 to 100000).foreach(i => {
      store.set(String.valueOf(i).getBytes(),
        String.valueOf(i).getBytes())
    })
    (1 to 100000).foreach(i => {
      assert(new String(store.get(String.valueOf(i).getBytes())) == String.valueOf(i))
    })
    (1 to 100000).foreach(i => {
      store.del(String.valueOf(i).getBytes())
    })
    val end = System.currentTimeMillis()
    println(end - start)
    println(store.U.count())
    println(store.U.bytes())
  }

  def randomTest(): Unit = {
    val store = new RedisUStore
    println(store.U.count())
    println(store.U.bytes())
    (1 to 1).foreach(i => {
      store.sadd(String.valueOf(i % 2).getBytes(), String.valueOf(i).getBytes())
    })
    println(store.U.count())
    println(store.U.bytes())
    (1 to 1).foreach(i => {
      store.spop(String.valueOf(i % 2).getBytes())
    })
    println(store.U.count())
    println(store.U.bytes())
    println(store.infoKeys())
  }

  def main(args: Array[String]): Unit = {
    unitTest()
  }
}
