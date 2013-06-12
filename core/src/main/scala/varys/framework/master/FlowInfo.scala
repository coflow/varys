package varys.framework.master

import varys.framework.FlowDescription
import scala.collection.mutable.HashSet

private[varys] class FlowInfo(val desc: FlowDescription) {
  var source = desc.originHost
  var destination:String = null
  var currentBps = 0.0

  def setDestination(dstHost: String) {
    destination = dstHost
  }

  def getFlowSize() = desc.sizeInBytes

}
