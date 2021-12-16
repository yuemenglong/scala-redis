package io.github.yuemenglong.redis.store

import java.lang.management.ManagementFactory
import java.net.{DatagramSocket, InetAddress}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConversions._

//noinspection RedundantBlock
class RedisInfo(store: RedisUStore) extends Serializable {
  val start: Long = System.currentTimeMillis()
  val host: String = {
    val socket = new DatagramSocket
    try {
      socket.connect(InetAddress.getByName("8.8.8.8"), 10002)
      socket.getLocalAddress.getHostAddress
    } finally if (socket != null) socket.close()
  }
  val port: AtomicLong = new AtomicLong()
  val pid: String = ManagementFactory.getRuntimeMXBean.getName.split("@")(0)
  val conn: AtomicLong = new AtomicLong()

  def alive: Long = System.currentTimeMillis() - start

  private def memoryHuman(mem: Long): String = {
    if (mem < 1024) {
      s"${mem}B"
    } else if (mem < 1024 * 1024) {
      f"${1.0 * mem / 1024}%.2fKB"
    } else if (mem < 1024 * 1024 * 1024) {
      f"${1.0 * mem / 1024 / 1024}%.2fMB"
    } else {
      f"${1.0 * mem / 1024 / 1024 / 1024}%.2fGB"
    }
  }

  private def aliveStr: String = {
    val sec = alive / 1000
    val min = sec / 60
    val hour = min / 60
    val day = hour / 24
    val d = day match {
      case 0 => ""
      case t => s"${t}d"
    }
    val h = hour % 24 match {
      case 0 => ""
      case t => s"${t}h"
    }
    val m = min % 60 match {
      case 0 => ""
      case t => s"${t}m"
    }
    val s = sec % 60 match {
      case 0 => ""
      case t => s"${t}s"
    }
    s"${d}${h}${m}${s}"
  }

  def updateAndGet(): String = {
    val memory = store.infoMemory()
    val memoryJVM = Runtime.getRuntime.maxMemory()
    val memoryJVMUsed = Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory()
    val gcCount = ManagementFactory.getGarbageCollectorMXBeans.map(_.getCollectionCount).sum
    val gcTime = ManagementFactory.getGarbageCollectorMXBeans.map(_.getCollectionTime).sum
    val keys = store.infoKeys()
    val capacity = store.infoCapacity()
    Array(
      s"# Info",
      s"memory: ${memory}",
      s"memory_human: ${memoryHuman(memory)}",
      s"memory_jvm: ${memoryJVM}",
      s"memory_jvm_human: ${memoryHuman(memoryJVM)}",
      s"memory_jvm_used: ${memoryJVMUsed}",
      s"memory_jvm_used_human: ${memoryHuman(memoryJVMUsed)}",
      s"host: ${host}",
      s"port: ${port}",
      s"keys: ${keys}",
      s"capacity: ${capacity}",
      s"alive: ${aliveStr}",
      s"pid: ${pid}",
      s"conn: ${conn}",
      s"gc_count: ${gcCount}",
      s"gc_time: ${gcTime}",
      ""
    ).mkString("\r\n")
  }

  def toStringLine: String = toString.trim.replace("\r\n", ", ")
}
