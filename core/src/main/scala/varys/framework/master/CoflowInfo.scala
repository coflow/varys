package varys.framework.master

import varys.framework.{FlowDescription, CoflowDescription}
import java.util.Date
import akka.actor.ActorRef
import scala.collection.mutable.{ArrayBuffer, HashMap}

private[varys] class CoflowInfo(
    val startTime: Long,
    val id: String,
    val desc: CoflowDescription,
    val submitDate: Date,
    val driver: ActorRef)
{
  var state = CoflowState.WAITING
  var endTime = -1L
  
  private val idToFlow = new HashMap[String, ArrayBuffer[FlowDescription]]

  private var _retryCount = 0

  def retryCount = _retryCount

  def getFlowDescs(flowId: String): Option[ArrayBuffer[FlowDescription]] = {
    idToFlow.get(flowId)
  }

  def contains(flowId: String): Boolean = {
    idToFlow.contains(flowId)
  }

  def addFlow(flowDesc: FlowDescription) {
    if (!idToFlow.contains(flowDesc.id)) {
      idToFlow(flowDesc.id) = new ArrayBuffer[FlowDescription]
    }
    idToFlow(flowDesc.id) += flowDesc
  }

  def addDestination(flowId: String, flowDesc: FlowDescription, destHost: String) {
    // TODO: 
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
