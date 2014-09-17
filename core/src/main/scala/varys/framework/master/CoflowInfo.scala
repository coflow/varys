package varys.framework.master

import akka.actor.ActorRef

import java.util.Date
import java.util.concurrent.atomic._
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap, Map}

import varys.framework.{ClientInfo, CoflowDescription}

private[varys] class CoflowInfo(
    val startTime: Long,
    val id: Int,
    val desc: CoflowDescription,
    val parentClient: ClientInfo,
    val submitDate: Date,
    val actor: ActorRef) {
  
  private var _prevState = CoflowState.WAITING
  def prevState = _prevState
  
  private var _curState = CoflowState.WAITING
  def curState = _curState
  
  def changeState(nextState: CoflowState.Value) {
    _prevState = _curState
    _curState = nextState
  }

  var readyTime = -1L
  var endTime = -1L

  def markFinished(endState: CoflowState.Value) {
    changeState(endState)
    endTime = System.currentTimeMillis()
  }

  def duration: Long = {
    if (endTime != -1) {
      endTime - startTime
    } else {
      System.currentTimeMillis() - startTime
    }
  }
  
  override def toString: String = "CoflowInfo(" + id + "[" + desc + "], state=" + curState + ")"
}
