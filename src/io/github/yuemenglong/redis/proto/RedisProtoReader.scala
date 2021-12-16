package io.github.yuemenglong.redis.proto

import java.nio.ByteBuffer
import java.util

import scala.collection.mutable.ArrayBuffer

class RedisProtoReader {

  //  object ReaderState extends Enumeration {
  //    type ReadState = Value
  //    val ArgsCountP, ArgsCount, ArgsCountN,
  //    ArgDataLenP, ArgDataLen, ArgDataLenN,
  //    ArgData, ArgDataR, ArgDataN,
  //    ArgsTelnet, ArgsTelnetN = Value
  //  }

  private val ArgsCountP: Int = 0
  private val ArgsCount: Int = 1
  private val ArgsCountN: Int = 2

  private val ArgDataLenP: Int = 10
  private val ArgDataLen: Int = 11
  private val ArgDataLenN: Int = 12

  private val ArgData: Int = 20
  private val ArgDataR: Int = 21
  private val ArgDataN: Int = 22

  private val ArgsTelnet: Int = 30
  private val ArgsTelnetN: Int = 31

  //  import ReaderState._

  //  var state: ReaderState.Value = ArgsCountP

  var state: Int = ArgsCountP
  var argsCount: Int = 0
  var argsData: ArrayBuffer[Array[Byte]] = new ArrayBuffer[Array[Byte]]()

  var argCur: Array[Byte] = _
  var argCurLen: Int = 0
  var argCurPos: Int = 0

  val handlers = new Array[ByteBuffer => Unit](64)

  handlers(ArgsCountP) = buf => buf.get(buf.position()) match {
    case '*' => (state = ArgsCount, buf.get())
    case a if a.toChar.isLetter => state = ArgsTelnet
    case _ => throw new ProtocolExecption
  }
  handlers(ArgsCount) = buf => buf.get() match {
    case '\r' => state = ArgsCountN
    case b => argsCount = argsCount * 10 + b - '0'
  }
  handlers(ArgsCountN) = buf => buf.get() match {
    case '\n' => state = ArgDataLenP
    case _ => throw new ProtocolExecption
  }
  handlers(ArgDataLenP) = buf => buf.get() match {
    case '$' => state = ArgDataLen
    case _ => throw new ProtocolExecption
  }
  handlers(ArgDataLen) = buf => buf.get() match {
    case '\r' => state = ArgDataLenN
    case b => argCurLen = argCurLen * 10 + b - '0'
  }
  handlers(ArgDataLenN) = buf => buf.get() match {
    case '\n' =>
      argCur = new Array[Byte](argCurLen)
      state = ArgData
    case _ => throw new ProtocolExecption
  }
  handlers(ArgData) = buf => buf.remaining() >= argCurLen - argCurPos match {
    case true => // 按长度读取,放入argsData,重置读取,改变状态
      buf.get(argCur, argCurPos, argCurLen - argCurPos)
      argsData += argCur
      resetCur()
      state = ArgDataR
    case false => // 剩余的全部读取,状态不变
      val read = buf.remaining()
      buf.get(argCur, argCurPos, buf.remaining())
      argCurPos += read
  }
  handlers(ArgDataR) = buf => buf.get() match {
    case '\r' => state = ArgDataN
    case _ => throw new ProtocolExecption
  }
  handlers(ArgDataN) = buf => buf.get() match {
    case '\n' => argsCount == argsData.length match {
      case false => state = ArgDataLenP //继续读
      case true => state = ArgsCountP // 本次已完成
      //        return getDataAndReset()
    }
    case _ => throw new ProtocolExecption
  }
  handlers(ArgsTelnet) = buf => {
    val len = (buf.position() until buf.limit()).find(pos => buf.get(pos) == '\r') match {
      case Some(pos) => pos - buf.position()
      case None => buf.remaining()
    }
    val data = new Array[Byte](len)
    buf.get(data)
    argsData += data
    if (buf.remaining() > 0) {
      buf.get()
      state = ArgsTelnetN
    }
  }
  handlers(ArgsTelnetN) = buf => buf.get() match {
    case '\n' =>
      val args = new String(argsData.toArray.flatten).split(" ").filter(_.nonEmpty).map(_.getBytes())
      argsData.clear()
      argsData ++= args
      state = ArgsCountP
    //      return new String(reset().flatten).split(" ").map(_.getBytes())
    case _ => throw new ProtocolExecption
  }

  def read(buffers: util.Deque[ByteBuffer]): Stream[Array[Array[Byte]]] = {
    Stream.continually(proc(buffers)).takeWhile(_ != null)
    //    buf = buf match {
    //      case null => buffer
    //      case _ =>
    //        val bs = new ByteArrayOutputStream()
    //        bs.write(buf.array(), buf.position(), buf.remaining())
    //        bs.write(buffer.array(), buffer.position(), buffer.remaining())
    //        ByteBuffer.wrap(bs.toByteArray)
    //    }
    //    val ret = Stream.continually(proc()).takeWhile(_ != null)
    //    if (buf.remaining() == 0) {
    //      buf = null
    //    }
    //    ret
  }

  def fetchDataAndReset(): Array[Array[Byte]] = {
    val ret = argsData.toArray
    //    state = ArgsCountP
    argsCount = 0
    argsData.clear
    ret
  }

  def resetCur(): Unit = {
    argCur = null
    argCurLen = 0
    argCurPos = 0
  }

  def proc(buffers: util.Deque[ByteBuffer]): Array[Array[Byte]] = {
    while (buffers.size() > 0) {
      val buf = buffers.peekFirst()
      while (buf != null && buf.remaining() > 0) {
        val oldState = state
        handlers(state)(buf)
        if (oldState != state && state == ArgsCountP) {
          // 状态改变可以返回
          return fetchDataAndReset()
        }
        //        state match {
        //          case ArgsCountP => buf.get(buf.position()) match {
        //            case '*' => (state = ArgsCount, buf.get())
        //            case a if a.toChar.isLetter => state = ArgsTelnet
        //            case _ => throw new ProtocolExecption
        //          }
        //          case ArgsCount => buf.get() match {
        //            case '\r' => state = ArgsCountN
        //            case b => argsCount = argsCount * 10 + b - '0'
        //          }
        //          case ArgsCountN => buf.get() match {
        //            case '\n' => state = ArgDataLenP
        //            case _ => throw new ProtocolExecption
        //          }
        //          case ArgDataLenP => buf.get() match {
        //            case '$' => state = ArgDataLen
        //            case _ => throw new ProtocolExecption
        //          }
        //          case ArgDataLen => buf.get() match {
        //            case '\r' => state = ArgDataLenN
        //            case b => argCurLen = argCurLen * 10 + b - '0'
        //          }
        //          case ArgDataLenN => buf.get() match {
        //            case '\n' =>
        //              argCur = new Array[Byte](argCurLen)
        //              state = ArgData
        //            case _ => throw new ProtocolExecption
        //          }
        //          case ArgData =>
        //            buf.remaining() >= argCurLen match {
        //              case true => // 按长度读取,放入argsData,重置读取,改变状态
        //                buf.get(argCur, argCurPos, argCurLen - argCurPos)
        //                argsData += argCur
        //                resetCur()
        //                state = ArgDataR
        //              case false => // 剩余的全部读取,状态不变
        //                val read = buf.remaining()
        //                buf.get(argCur, argCurPos, buf.remaining())
        //                argCurPos += read
        //            }
        //          //          case ArgData => buf.remaining() >= argLen match {
        //          //            case true =>
        //          //              val arg = new Array[Byte](argLen)
        //          //              buf.get(arg)
        //          //              argLen = 0
        //          //              argsData += arg
        //          //              state = ArgDataR
        //          //            case false => return null // 等数据全了在返回
        //          //          }
        //          case ArgDataR => buf.get() match {
        //            case '\r' => state = ArgDataN
        //            case _ => throw new ProtocolExecption
        //          }
        //          case ArgDataN => buf.get() match {
        //            case '\n' => argsCount == argsData.length match {
        //              case false => state = ArgDataLenP //继续读
        //              case true => return getDataAndReset()
        //            }
        //            case _ => throw new ProtocolExecption
        //          }
        //          case ArgsTelnet =>
        //            val len = (buf.position() until buf.limit()).find(pos => buf.get(pos) == '\r') match {
        //              case Some(pos) => pos - buf.position()
        //              case None => buf.remaining()
        //            }
        //            val data = new Array[Byte](len)
        //            buf.get(data)
        //            argsData += data
        //            if (buf.remaining() > 0) {
        //              buf.get()
        //              state = ArgsTelnetN
        //            }
        //          case ArgsTelnetN => buf.get() match {
        //            case '\n' => return new String(getDataAndReset().flatten).split(" ").map(_.getBytes())
        //            case _ => throw new ProtocolExecption
        //          }
        //        }
      }
      if (buf.remaining() == 0) {
        buffers.pollFirst()
      }
    }
    null
  }
}
