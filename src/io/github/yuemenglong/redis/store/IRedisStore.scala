package io.github.yuemenglong.redis.store

case class MemoryLimitException() extends RuntimeException("Memory Limit")

case class WrongTypeException(cmd: String, key: Array[Byte], obj: Object)
  extends RuntimeException(String.format(
    "WrongType: %s %s, Type: %s",
    cmd, key, obj.getClass.getSimpleName))

trait IRedisStore {

  def set(key: Array[Byte], value: Array[Byte]): Unit

  def setnx(key: Array[Byte], value: Array[Byte]): Int

  def get(key: Array[Byte]): Array[Byte]

  def del(key: Array[Byte]): Int

  def sadd(key: Array[Byte], value: Array[Byte]): Int

  def spop(key: Array[Byte]): Array[Byte]

  def smember(key: Array[Byte]): Array[Array[Byte]]

  def scard(key: Array[Byte]): Long

  def srem(key: Array[Byte], value: Array[Byte]): Int

  def hset(key: Array[Byte], hkey: Array[Byte], value: Array[Byte]): Int

  def hget(key: Array[Byte], hkey: Array[Byte]): Array[Byte]

  def hdel(key: Array[Byte], hkey: Array[Byte]): Int

  def hgetAll(key: Array[Byte]): Array[Array[Byte]]

  def lpush(key: Array[Byte], value: Array[Byte]): Int

  def rpush(key: Array[Byte], value: Array[Byte]): Int

  def lpop(key: Array[Byte]): Array[Byte]

  def rpop(key: Array[Byte]): Array[Byte]

  def llen(key: Array[Byte]): Int

  def incr(key: Array[Byte]): Long

  def incrBy(key: Array[Byte], value: Array[Byte]): Long

  def decr(key: Array[Byte]): Long

  def decrBy(key: Array[Byte], value: Array[Byte]): Long

  def keys(pat: Array[Byte]): Seq[String]

  def flushAll(): Unit

  def info(): RedisInfo

  def infoKeys(): Long

  def infoMemory(): Long

  def infoCapacity(): Long
}
