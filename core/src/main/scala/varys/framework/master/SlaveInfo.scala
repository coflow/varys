package varys.framework.master

import akka.actor.ActorRef

import scala.collection.mutable

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
  
  override def toString: String = "SlaveInfo(" + id + "[" + host + ":" + port + "]:" + state + ")"
}
