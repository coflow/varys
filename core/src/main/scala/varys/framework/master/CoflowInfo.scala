package varys.framework.master

import varys.framework.{FlowDescription, CoflowDescription}
import java.util.Date
import akka.actor.ActorRef
import scala.collection.mutable.{HashMap, HashSet}

private[varys] class CoflowInfo(
    val startTime: Long,
    val id: String,
    val desc: CoflowDescription,
    val submitDate: Date,
    val driver: ActorRef)
{
  var state = CoflowState.WAITING
  var endTime = -1L
  
  var flows = new HashSet[FlowInfo]
  val idToFlow = new HashMap[String, FlowInfo]

  private var _retryCount = 0

  def retryCount = _retryCount

  def getFlowDesc(flowId: String): FlowDescription = {
    idToFlow(flowId).desc
  }

  def contains(flowId: String): Boolean = {
    idToFlow.contains(flowId)
  }

  def addFlow(flowDesc: FlowDescription) {
    val flow = new FlowInfo(flowDesc)
    flows += flow
    idToFlow(flowDesc.id) = flow
  }

  def addDestination(flowId: String, destHost: String) {
    val flow = idToFlow(flowId)
    flow.addDestination(destHost)
  }

  def removeFlow(flowId: String) {
    // TODO: 
  }

  def incrementRetryCount = {
    _retryCount += 1
    _retryCount
  }

  def markFinished(endState: CoflowState.Value) {
    state = endState
    endTime = System.currentTimeMillis()
  }

  def duration: Long = {
    if (endTime != -1) {
      endTime - startTime
    } else {
      System.currentTimeMillis() - startTime
    }
  }
}
