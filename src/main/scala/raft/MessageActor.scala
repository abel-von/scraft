package raft

import akka.actor.{Actor, ActorIdentity, ActorLogging, ActorRef, Identify, ReceiveTimeout}

import scala.concurrent.duration._

/**
  * Created by pheng on 2017/11/19.
  */

case class SendToAll(msg: Any)

class MessageActor(val id: String, val members: Map[String, String]) extends Actor with ActorLogging {

  var memberActors: Map[String, ActorRef] = Map.empty[String, ActorRef]
  memberActors += (id -> context.parent)
  initMemberActors

  def initMemberActors = {
    log.debug(s"Init member actors of $members")
    members.foreach {
      case (memberId: String, address: String) if memberId != id =>
        log.debug(s"Member ID: $memberId, Address: $address")
        //if (!memberActors.contains(memberId)) {
          val path = s"akka.tcp://Raft@$address/user/$memberId"
          log.debug(s"Identify actor of $memberId with address $address")
          context.actorSelection(path) ! Identify(path)
        //} else {
          //log.debug("actor has been ok")
        //}
      case _ =>
    }
    if (members.exists(kv => !memberActors.contains(kv._1))) {
      import context.dispatcher
      context.system.scheduler.scheduleOnce(5.seconds, self, ReceiveTimeout)
    }
  }

  override def receive: Receive = {
    case ActorIdentity(path, Some(actor)) =>
      log.debug(s"Remote actor available: $path")
      memberActors += (path.toString -> actor)
    case ActorIdentity(path, None) =>
      log.warning(s"Remote actor not available: $path")
      memberActors -= path.toString
    case ReceiveTimeout => initMemberActors
    case SendToAll(msg) =>
      log.debug(s"Send $msg to all members $memberActors")
      memberActors foreach (_._2 ! msg)
    case resp => {
      context.parent ! resp
    }
  }
}
