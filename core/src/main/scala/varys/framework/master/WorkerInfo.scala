package varys.framework.master

import akka.actor.ActorRef
import scala.collection.mutable

private[varys] class SlaveInfo(
  val id: String,
  val host: String,
  val port: Int,
  val cores: Int,
  val memory: Int,
  val actor: ActorRef,
  val webUiPort: Int,
  val publicAddress: String) {

  var state: SlaveState.Value = SlaveState.ALIVE
  var coresUsed = 0
  var memoryUsed = 0

  var lastHeartbeat = System.currentTimeMillis()

  def coresFree: Int = cores - coresUsed
  def memoryFree: Int = memory - memoryUsed

  def webUiAddress : String = {
    "http://" + this.publicAddress + ":" + this.webUiPort
  }

  def setState(state: SlaveState.Value) = {
    this.state = state
  }
}
