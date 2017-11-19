package raft

import akka.actor.{Actor, ActorLogging, FSM, Props}

import scala.concurrent.duration._
import scala.util.Random

/**
  * Created by pheng on 2017/11/12.
  */

sealed trait RaftNodeState

object Candidate extends RaftNodeState

object Follower extends RaftNodeState

object Leader extends RaftNodeState

final case class StatusData(term: Int,
                            leaderId: String = "",
                            votes: Map[String, Boolean] = Map.empty)

final case class Vote(id: String, term: Int, logTerm: Int, logIndex: Int)

final case class Heartbeat(id: String, term: Int)

final case class VoteResponse(id: String, grant: Boolean)

final case class Append(entry: Entry)

final case class Propose(entry: Entry)

object Campaign

object Beat

trait Entry

class RaftNode(id: String,
               members: Map[String, String]) extends
  Actor with
  FSM[RaftNodeState, StatusData] with
  ActorLogging {

  log.info(s"Member $id starting")
  val messageActorRef = context.actorOf(Props(classOf[MessageActor], id, members))
  var heartbeatElapsed = 0

  startWith(Candidate, StatusData(0))
  setTimer(s"champaign-timer-$id", Campaign, 5.seconds + Random.nextInt(5).seconds, repeat = false)

  when(Candidate) {
    case Event(vote@Vote(voteId, voteTerm, logTerm, logIndex), StatusData(selfTerm, leaderId, _))
      if shouldGrant(vote, selfTerm) =>
      sender() ! VoteResponse(id, grant = true)
      log.info(s"Received vote from sender ${sender()}")
      cancelTimer(s"champaign-timer-$id")
      setTimer(s"beat-timer-$id", Beat, 1 seconds, repeat = false)
      heartbeatElapsed = 0
      log.info(s"Member $id become follower because received the vote of $voteId with the term $voteTerm")
      goto(Follower) using StatusData(voteTerm, voteId)
    case Event(VoteResponse(voterId, true), StatusData(term, _, votes)) =>
      val newVotes = votes + (voterId -> true)
      log.info(s"Member $id received vote from $voterId")
      val grantCount = newVotes.count(_._2)
      log.info(s"Member $id received $grantCount votes")
      if (grantCount > members.size / 2) {
        log.info(s"$id become leader")
        cancelTimer(s"campaign-timer-$id")
        setTimer(s"beat-timer-$id", Beat, 1 seconds, repeat = false)
        goto(Leader) using StatusData(term, id, newVotes)
      } else {
        stay() using StatusData(term, id, newVotes)
      }
    case Event(Campaign, stateData) =>
      log.info(s"Member $id start campaign")
      val newVotes = Map.empty + (id -> true)
      val newStateData = stateData.copy(term = stateData.term + 1, votes = newVotes)
      sendVotesToAll(newStateData.term)
      setTimer(s"champaign-timer-$id", Campaign, 5.seconds + Random.nextInt(5).seconds, repeat = false)
      stay() using newStateData
    case Event(Heartbeat(fromId, term), stateData) if term > stateData.term =>
      log.info(s"Received messages from a new leader $fromId with higher term")
      heartbeatElapsed = 0
      goto(Follower) using StatusData(term, fromId)
    case e =>
      log.info(s"Member $id received event $e, and do nothing")
      stay()
  }

  when(Follower) {
    case Event(Append(entry), stateData) =>
      stay()
    case Event(vote@Vote(voteId, term, logTerm, logIndex), StatusData(selfTerm, leaderId, _)) =>
      log.info(s"Member $id received campaign from $voteId: $vote")
      if (shouldGrant(vote, selfTerm)) {
        log.info(s"Member $id received campaign from $voteId and vote for it")
        sender() ! VoteResponse(id, grant = true)
        stay() using StatusData(term, voteId)
      } else {
        log.info(s"Member $id received campaign from $voteId and reject it")
        sender() ! VoteResponse(id, grant = false)
        stay()
      }
    case Event(Heartbeat(fromId, term), stateData) if fromId == stateData.leaderId =>
      log.info(s"Member $id received heartbeat from leader")
      heartbeatElapsed = 0
      stay()
    case Event(Heartbeat(fromId, term), stateData) if term > stateData.term =>
      log.info(s"Received messages from a new leader $fromId with higher term")
      stay() using StatusData(term, fromId)
    case Event(Beat, stateData) =>
      heartbeatElapsed = heartbeatElapsed + 1
      if (heartbeatElapsed >= RaftNode.HEARTBEAT_TIMEOUT) {
        log.info(s"Member $id waited timeout, become candidate and start new campaign")
        cancelTimer(s"beat-timer-$id")
        setTimer(s"champaign-timer-$id", Campaign, 5.seconds + Random.nextInt(5).seconds, repeat = false)
        goto(Candidate) using StatusData(stateData.term)
      } else {
        setTimer(s"beat-timer-$id", Beat, 1 seconds, repeat = false)
        stay()
      }
  }

  when(Leader) {
    case Event(Propose(entry), _) =>
      stay()
    case Event(vote@Vote(voteId, term, logTerm, logIndex), StatusData(selfTerm, leaderId, _))
      if shouldGrant(vote, selfTerm) =>
      sender() ! VoteResponse(id, grant = true)
      log.info(s"Member $id become follower because received campaign from $voteId")
      heartbeatElapsed = 0
      goto(Follower) using StatusData(term, voteId)
    case Event(Beat, stateData) =>
      messageActorRef ! SendToAll(Heartbeat(id, stateData.term))
      setTimer(s"beat-timer-$id", Beat, 1 seconds, repeat = false)
      stay()
    case Event(Heartbeat(fromId, term), stateData) if term > stateData.term =>
      log.info(s"Received messages from a new leader $fromId with higher term")
      heartbeatElapsed = 0
      goto(Follower) using StatusData(term, fromId)
    case Event(Heartbeat(fromId, _), stateData) if fromId == this.id =>
      stay()
  }

  private def shouldGrant(vote: Vote, currentTerm: Int): Boolean = {
    if (vote.term <= currentTerm) false
    else true
  }

  private def sendVotesToAll(currentTerm: Int): Unit = {
    log.debug(s"$id send votes to all members")
    messageActorRef ! SendToAll(Vote(id, currentTerm, 0, 0))
  }
}

object RaftNode {
  val HEARTBEAT_TIMEOUT = 5
}