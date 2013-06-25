package varys.framework.master

import varys.framework.FlowDescription
import scala.collection.mutable.HashSet

private[varys] class FlowInfo(val desc: FlowDescription) {
  var source = desc.originHost
  var destClient:ClientInfo = null
  var currentBps = 0.0
  var lastScheduled: Long = 0L

  def setDestination(dClient: ClientInfo) {
    destClient = dClient
  }

  def getFlowSize() = desc.sizeInBytes

  override def toString:String = "FlowInfo(" + source + "-->" + destClient.host + "[" + desc + "] @ " + currentBps + " bps)"
}
