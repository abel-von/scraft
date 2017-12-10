package wal

import akka.actor.{ActorSystem, PoisonPill, Props, Terminated}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

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
      val actorRef = system.actorOf(Props(classOf[LogActor], "test-log", new TestCommandSerializer), "test-log-actor")
      actorRef ! LogEntry(1, new TestCommand("put"))
      actorRef ! PoisonPill
      val actorRef1 = system.actorOf(Props(classOf[LogActor], "test-log", new TestCommandSerializer), "test-log-actor-2")
      implicit val timeout = Timeout(5 seconds)
      val logs = Await.result((actorRef1 ? CurrentLogs).mapTo[Seq[LogEntry]], 5.seconds)
      assert(logs.head.index == 1)
      assert(logs.head.cmd.asInstanceOf[TestCommand].str == "put")
    }
  }
  "LogActor" must {
    "cut the file when logs is too much" in {
      val actorRef = system.actorOf(Props(classOf[LogActor], "test-log", new TestCommandSerializer), "test-log-actor")
      val randomCommandStr = "adsf" * 10240 *1024
      actorRef ! LogEntry(1, new TestCommand(randomCommandStr))
      actorRef ! LogEntry(2, new TestCommand(randomCommandStr))
      actorRef ! LogEntry(3, new TestCommand("put"))
      actorRef ! PoisonPill
      val probe = TestProbe()
      probe.watch(actorRef)
      probe.expectMsgType[Terminated](60.seconds)
      val actorRef1 = system.actorOf(Props(classOf[LogActor], "test-log", new TestCommandSerializer), "test-log-actor-2")
      implicit val timeout = Timeout(20 seconds)
      val logs = Await.result((actorRef1 ? CurrentLogs).mapTo[Seq[LogEntry]], 20.seconds)
      assert(logs.head.index == 2)
      assert(logs(1).cmd.asInstanceOf[TestCommand].str == "put")
    }
  }
}
