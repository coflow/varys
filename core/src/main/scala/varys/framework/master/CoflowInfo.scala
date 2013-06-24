package varys.framework.master

import akka.actor.ActorRef

import java.util.Date
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.{ArrayBuffer, HashMap}

import varys.framework.{FlowDescription, CoflowDescription}

private[varys] class CoflowInfo(
    val startTime: Long,
    val id: String,
    val desc: CoflowDescription,
    val submitDate: Date,
    val actor: ActorRef)
{
  var state = CoflowState.WAITING
  var endTime = -1L
  var alpha = 0.0
  
  private val idToFlow = new HashMap[String, FlowInfo]

  private var _retryCount = 0

  def retryCount = _retryCount

  private val numRegisteredFlows = new AtomicInteger(0)
  def getNumRegisteredFlows = numRegisteredFlows.get
  def getFlows() = idToFlow.values.filter(_.destClient != null)

  def getFlowInfo(flowId: String): Option[FlowInfo] = {
    idToFlow.get(flowId)
  }

  def contains(flowId: String) = idToFlow.contains(flowId)

  /**
   * Calculating static alpha for the coflow
   */
  private def calcAlpha(): Double = {
    val sBytes = new HashMap[String, Double]().withDefaultValue(0.0)
    val rBytes = new HashMap[String, Double]().withDefaultValue(0.0)

    getFlows.foreach { flowInfo =>
      // FIXME: Assuming a single source and destination for each flow
      val src = flowInfo.source
      val dst = flowInfo.destClient.host
      
      sBytes(src) = sBytes(src) + flowInfo.desc.sizeInBytes
      rBytes(dst) = rBytes(dst) + flowInfo.desc.sizeInBytes
    }
    math.max(sBytes.values.max, rBytes.values.max)
  }

  def addFlow(flowDesc: FlowDescription) {
    assert(!idToFlow.contains(flowDesc.id))
    idToFlow(flowDesc.id) = new FlowInfo(flowDesc)
  }

  /**
   * Adds destination for a given piece of data. 
   * Returns true if the coflow is ready to go
   */
  def addDestination(flowId: String, destClient: ClientInfo): Boolean = {
    if (!idToFlow.contains(flowId)) {
      numRegisteredFlows.incrementAndGet
    }
    idToFlow(flowId).setDestination(destClient)
    
    // Mark this coflow as RUNNING only after all flows are alive
    if (getNumRegisteredFlows == desc.maxFlows) {
      state = CoflowState.READY
      alpha = calcAlpha()
      true
    } else {
      false
    }
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

  /**
   * Returns an estimation of remaining bytes
   */
  def remainingSizeInBytes(): Double = {
    1E6
  }

  def duration: Long = {
    if (endTime != -1) {
      endTime - startTime
    } else {
      System.currentTimeMillis() - startTime
    }
  }
  
  override def toString: String = "CoflowInfo(" + id + "[" + desc + "]:" + state + ":" + getNumRegisteredFlows + ")"
}
