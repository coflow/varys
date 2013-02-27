package varys.framework.master

import varys.framework.FlowDescription
import scala.collection.mutable.HashSet

private[varys] class FlowInfo(val desc: FlowDescription) {
  var sources = new HashSet[String]
  var destinations = new HashSet[String]

  sources += desc.originHost

  def addSource(slaveId: String) {
    sources += slaveId
  }

  def removeSource(slaveId: String) {
    sources -= slaveId
  }

  def addDestination(slaveId: String) {
    destinations += slaveId
  }

  def removeDestination(slaveId: String) {
    destinations -= slaveId
  }

}
