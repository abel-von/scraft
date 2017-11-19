package kv

import org.scalatest.FunSuite
import wal.CommandSerializer

/**
  * Created by pheng on 2017/12/2.
  */
class CommandTest extends FunSuite {
  test("serializer should be reversable") {
    val serializer: CommandSerializer[KVCommand] = new KVCommandSerializer
    val bytes = serializer.cmdToBytes(new KVCommand(CommandType.GET, "/aaa", null))
    val cmd = serializer.bytesToCmd(bytes)
    assert(cmd.t == CommandType.GET)
    assert(cmd.key == "/aaa")
    assert(cmd.value == null)

    val bytes2 = serializer.cmdToBytes(new KVCommand(CommandType.PUT, "/aaa", "bbbb".toCharArray.map(_.toByte)))
    val cmd2 = serializer.bytesToCmd(bytes2)
    assert(cmd2.t == CommandType.PUT)
    assert(cmd2.key == "/aaa")
    assert(new String(cmd2.value) == "bbbb")
  }
}
