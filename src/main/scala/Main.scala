import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import raft.RaftNode

/**
  * Created by pheng on 2017/11/3.
  */
object Main {
  def main(args: Array[String]) {

    val (memberId: String, port: String) = args match {
      case Array(id, port) => {
        (id, port)
      }
      case Array(id) => {
        (id, "0")
      }
      case _ => {
        println("args is not legal")
        System.exit(1)
      }
    }
    println(s"id: ${memberId}, port: ${port}")
    val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port").
      withFallback(ConfigFactory.load("raft"))
    val system = ActorSystem("Raft", config)

    val members = (1 to 3).map(i => i.toString -> s"127.0.0.1:400$i").toMap
    system.actorOf(Props(classOf[RaftNode], memberId, members), name = memberId)
  }
}
