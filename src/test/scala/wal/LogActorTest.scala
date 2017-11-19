package wal

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

/**
  * Created by pheng on 2017/12/2.
  */
class TestCommand(val str: String) extends Command

class TestCommandSerializer extends CommandSerializer[TestCommand] {
  override def cmdToBytes(cmd: TestCommand): Array[Byte] = {
    cmd.str.toCharArray.map(_.toByte)
  }

  override def bytesToCmd(bytes: Array[Byte]): TestCommand = {
    new TestCommand(String.valueOf(bytes.map(_.toChar)))
  }
}

class LogActorTest() extends TestKit(ActorSystem("log")) with
  ImplicitSender with
  WordSpecLike with
  Matchers with
  BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "LogActor" must {
    "store the log to the file" in {
      val actorRef = system.actorOf(Props(classOf[LogActor],"test-log",new TestCommandSerializer),"test-log-actor")
      actorRef ! LogEntry(1,new TestCommand("put"))
    }
  }
}
