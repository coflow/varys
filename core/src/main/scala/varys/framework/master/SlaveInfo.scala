package varys.framework.master

import akka.actor.ActorRef
import scala.collection.mutable

private[varys] class SlaveInfo(
  val id: String,
  val host: String,
  val port: Int,
  val actor: ActorRef,
  val webUiPort: Int,
  val publicAddress: String) {

  var state: SlaveState.Value = SlaveState.ALIVE

  var lastHeartbeat = System.currentTimeMillis()

  val rxBpsInfo = new BpsInfo()
  val txBpsInfo = new BpsInfo()

  def rxBps = rxBpsInfo.bps
  def txBps = txBpsInfo.bps

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
}
