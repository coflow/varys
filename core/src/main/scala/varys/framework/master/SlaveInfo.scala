package varys.framework.master

import akka.actor.ActorRef

import scala.collection.mutable.{ArrayBuffer, HashMap}

import varys.util.BpsInfo

private[varys] class SlaveInfo(
  val id: String,
  val host: String,
  val port: Int,
  val actor: ActorRef,
  val webUiPort: Int,
  val commPort: Int,
  val publicAddress: String) {

  var state: SlaveState.Value = SlaveState.ALIVE

  var lastHeartbeat = System.currentTimeMillis()

  val rxBpsInfo = new BpsInfo()
  val txBpsInfo = new BpsInfo()

  def rxBps = rxBpsInfo.getBps
  def txBps = txBpsInfo.getBps

  def webUiAddress : String = {
    "http://" + this.publicAddress + ":" + this.webUiPort
  }

  def setState(state: SlaveState.Value) = {
    this.state = state
  }
  
  def updateNetworkStats(newRxBps: Double, newTxBps: Double) = {
    rxBpsInfo.update(newRxBps)
    txBpsInfo.update(newTxBps)
  }

  var numCoflows = 0
  var coflowIds: Array[String] = null
  var sizes: Array[Long] = null
  var flows: Array[Array[String]] = null
  val localCoflows = new HashMap[String, (Long, Array[String])]()

  def updateCoflows(
      coflowIds_ : Array[String], 
      sizes_ : Array[Long], 
      flows_ : Array[Array[String]]) {

    numCoflows = coflowIds_.size
    coflowIds = coflowIds_
    sizes = sizes_
    flows = flows_

    localCoflows.clear
    for (i <- 0 until numCoflows) {
      localCoflows(coflowIds(i)) = ((sizes(i), flows(i)))
    }
  }

  var lastSchedule: String = null
  def sameAsLastSchedule(newCoflowOrder: String, newSchedule: ArrayBuffer[String]): Boolean = {
    val ns = newCoflowOrder + " <> " + scala.util.Sorting.stableSort(newSchedule).mkString("|")
    if (lastSchedule == null) {
      lastSchedule = ns
      return true
    } 

    val retVal = (lastSchedule == ns)
    lastSchedule = ns
    retVal
  }

  override def toString: String = "SlaveInfo(" + id + "[" + host + ":" + port + "]:" + state + ")"
}
