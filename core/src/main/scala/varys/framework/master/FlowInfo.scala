package varys.framework.master

import varys.framework.FlowDescription
import scala.collection.mutable.HashSet

private[varys] class FlowInfo(val desc: FlowDescription) {
  var sources = new HashSet[String]
  var destinations = new HashSet[String]

  sources += desc.originHost

  def addSource(srcHost: String) {
    sources += srcHost
  }

  def removeSource(srcHost: String) {
    sources -= srcHost
  }

  def addDestination(dstHost: String) {
    destinations += dstHost
  }

  def removeDestination(dstHost: String) {
    destinations -= dstHost
  }

}
