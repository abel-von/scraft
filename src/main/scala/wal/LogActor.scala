package wal

import java.io.{File, RandomAccessFile}
import java.nio.channels.FileChannel.MapMode

import akka.actor.{Actor, ActorLogging}

import scala.annotation.tailrec

/**
  * Created by pheng on 2017/11/26.
  */
case class LogEntry(index: Long, cmd: Command)

trait Command

trait CommandSerializer[T <: Command] {
  def cmdToBytes(cmd: T): Array[Byte]

  def bytesToCmd(bytes: Array[Byte]): T
}

class LogActor(path: String, cs: CommandSerializer[Command]) extends Actor with ActorLogging {
  val f = new File(path)
  val exists = f.exists()
  val fc = new RandomAccessFile(new File(path), "rw").getChannel
  val buf = fc.map(MapMode.READ_WRITE, 0, LogActor.BUFFER_SIZE)
  val bufferedLogs: Seq[LogEntry] = if (exists) {
    replay(Seq.empty[LogEntry])
  } else {
    Seq.empty
  }

  @tailrec
  private def replay(logs: Seq[LogEntry]): Seq[LogEntry] = {
    val length = buf.getInt()
    if (length <= 0) {
      buf.position(buf.position() - Integer.BYTES)
      logs
    } else {
      val index = buf.getLong
      var newLogs = logs
      if (logs.nonEmpty && index <= logs.last.index) {
        newLogs = logs.filter(_.index < index)
      }
      val bytes = new Array[Byte](length - java.lang.Long.BYTES)
      buf.get(bytes)
      val cmd = cs.bytesToCmd(bytes)
      replay(newLogs :+ LogEntry(index, cmd))
    }
  }

  override def receive: Receive = {
    case LogEntry(index, cmd) =>
      log.info(s"Received log entry of index:$index")
      val bytes = cs.cmdToBytes(cmd)
      buf.putInt(bytes.length + java.lang.Long.BYTES)
      buf.putLong(index)
      buf.put(bytes)
      buf.force()
  }

}

object LogActor {
  val BUFFER_SIZE = 64 * 1024 * 1024
}
