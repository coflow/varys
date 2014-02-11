package varys.framework.master

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.HashSet

import varys.framework.FlowDescription

private[varys] class FlowInfo(
    val desc: FlowDescription) {
  
  var source = desc.originHost
  var destClient:ClientInfo = null
  var currentBps = 0.0
  var lastScheduled: Long = 0L

  var bytesLeft_ = new AtomicLong(desc.sizeInBytes)
  def bytesLeft: Long = bytesLeft_.get()

  def setDestination(dClient: ClientInfo) {
    destClient = dClient
  }

  def isLive = (destClient != null && bytesLeft > 0)
  
  def getFlowSize() = desc.sizeInBytes
  def decreaseBytes(byteToDecrease: Long) { 
    bytesLeft_.getAndAdd(-byteToDecrease) 
  }

  override def toString:String = "FlowInfo(" + source + " --> " + destClient.host + "[" + desc + 
    "], bytesLeft=" + bytesLeft + ", currentBps=" + currentBps + ")"
}
