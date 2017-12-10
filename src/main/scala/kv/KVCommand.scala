package kv

import java.nio.ByteBuffer

import kv.CommandType.CommandType

/**
  * Created by pheng on 2017/12/2.
  */
object CommandType extends Enumeration {
  type CommandType = Value
  val GET = Value(1)
  val PUT = Value(2)
  val DELETE = Value(3)

  implicit def toInt(t: CommandType): Int = t.id
}

class KVCommand(val t: CommandType, val key: String, val value: Array[Byte]) extends wal.Command

class KVCommandSerializer extends wal.CommandSerializer[KVCommand] {
  override def cmdToBytes(cmd: KVCommand): Array[Byte] = {
    if (cmd.value != null) {
      intToByteArray(cmd.t) ++ stringToByteArray(cmd.key) ++ intToByteArray(cmd.value.length) ++ cmd.value
    } else {
      intToByteArray(cmd.t) ++ stringToByteArray(cmd.key) ++ intToByteArray(0)
    }
  }

  override def bytesToCmd(bytes: Array[Byte]): KVCommand = {
    val cmdTypeInt = byteArrayToInt(bytes.slice(0, 4))
    val keyLen = byteArrayToInt(bytes.slice(4, 8))
    val keyEnd = 8 + keyLen
    val key = new String(bytes.slice(8, keyEnd))
    val valueLen = byteArrayToInt(bytes.slice(keyEnd, keyEnd + 4))
    if (valueLen == 0) {
      new KVCommand(CommandType(cmdTypeInt), key, null)
    } else {
      val value = bytes.slice(keyEnd + 4, keyEnd + 4 + valueLen)
      new KVCommand(CommandType(cmdTypeInt), key, value)
    }
  }

  def byteArrayToInt(bytes: Array[Byte]): Int = {
    bytes.slice(0, 4).foldLeft(0: Int)((a, t) => (a << 8) + t)
  }

  def intToByteArray(i: Int): Array[Byte] = {
    val buf = ByteBuffer.allocate(4)
    buf.putInt(i)
    buf.array()
  }

  def stringToByteArray(s: String): Array[Byte] = {
    val strBytes = s.foldLeft(Array.empty[Byte])(_ :+ _.toByte)
    intToByteArray(strBytes.length) ++ strBytes
  }

}
