package varys.framework

import akka.actor.ActorRef

import varys.framework.master.{CoflowInfo, SlaveInfo}

private[varys] sealed trait FrameworkMessage extends Serializable

// Slave to Master
private[varys] case class RegisterSlave(
    id: String,
    host: String,
    port: Int,
    webUiPort: Int,
    commPort: Int,
    publicAddress: String)
  extends FrameworkMessage

private[varys] case class Heartbeat(
    slaveId: String,
    rxBps: Double,
    txBps: Double)
  extends FrameworkMessage

private[varys] case class LocalCoflows(
    slaveId: String,
    coflowIds: Array[String],
    sizes: Array[Long],
    flows: Array[Array[String]])
  extends FrameworkMessage

// Master to Slave
private[varys] case class RegisteredSlave(
    masterWebUiUrl: String) 
  extends FrameworkMessage

private[varys] case class RegisterSlaveFailed(
    message: String) 
  extends FrameworkMessage

private[varys] case class GlobalCoflows(
    coflows: Array[String],
    sendTo: Array[String])
  extends FrameworkMessage

// Client to Master
private[varys] case class RegisterMasterClient(
    clientName: String, 
    host: String, 
    commPort: Int) 
  extends FrameworkMessage

private[varys] case class RegisterCoflow(
    clientId: String, 
    coflowDescription: CoflowDescription) 
  extends FrameworkMessage

private[varys] case class UnregisterCoflow(
    coflowId: String) 
  extends FrameworkMessage

private[varys] case class RequestBestRxMachines(
    howMany: Int, 
    adjustBytes: Long) 
  extends FrameworkMessage

private[varys] case class RequestBestTxMachines(
    howMany: Int, 
    adjustBytes: Long) 
  extends FrameworkMessage

// Client to Slave
private[varys] case class RegisterSlaveClient(
    coflowId: String, 
    clientName: String, 
    host: String, 
    commPort: Int) 
  extends FrameworkMessage

private[varys] case class StartedFlow(
    coflowId: String,
    sIPPort: String,
    dIPPort: String)
  extends FrameworkMessage

private[varys] case class CompletedFlow(
    coflowId: String,
    sIPPort: String,
    dIPPort: String)
  extends FrameworkMessage

private[varys] case class UpdateCoflowSize(
    coflowId: String,
    curSize: Long)
  extends FrameworkMessage

// Master/Client to Client/Slave
private[varys] case class RegisteredMasterClient(
    clientId: String, 
    slaveId: String,
    slaveUrl:String) 
  extends FrameworkMessage

private[varys] case class RegisteredSlaveClient(
    clientId: String) 
  extends FrameworkMessage

private[varys] case class CoflowKilled(
    message: String) 
  extends FrameworkMessage

// Master to Client
private[varys] case class RegisterClientFailed(
    message: String) 
  extends FrameworkMessage

private[varys] case class RegisteredCoflow(
    coflowId: String) 
  extends FrameworkMessage

private[varys] case class RegisterCoflowFailed(
    message: String) 
  extends FrameworkMessage

private[varys] case class UnregisteredCoflow(
    coflowId: String) 
  extends FrameworkMessage

private[varys] case class RejectedCoflow(
    coflowId: String,
    message: String) 
  extends FrameworkMessage

private[varys] case class BestRxMachines(
    bestRxMachines: Array[String]) 
  extends FrameworkMessage

private[varys] case class BestTxMachines(
    bestTxMachines: Array[String]) 
  extends FrameworkMessage

// Internal message in Client
private[varys] case object StopClient

private[varys] case object RegisterWithMaster

// Internal message in Slave

// Internal message in Master
private[varys] case object CheckForSlaveTimeOut

private[varys] case object SyncSlaves

private[varys] case object RequestWebUIPort

private[varys] case class WebUIPortResponse(webUIBoundPort: Int)

// MasterWebUI To Master
private[varys] case object RequestMasterState

// Master to MasterWebUI
private[varys] case class MasterState(
    host: String, 
    port: Int, 
    slaves: Array[SlaveInfo],
    activeCoflows: Array[CoflowInfo], 
    completedCoflows: Array[CoflowInfo],
    activeClients: Array[ClientInfo]) {

  def uri = "varys://" + host + ":" + port
}

//  SlaveWebUI to Slave
private[varys] case object RequestSlaveState

// Slave to SlaveWebUI
private[varys] case class SlaveState(
    host: String, 
    port: Int, 
    slaveId: String, 
    masterUrl: String, 
    rxBps: Double,
    txBps: Double,
    masterWebUiUrl: String)

// Slave to Client
private[varys] case object PauseAll

private[varys] case object StartAll

private[varys] case class PauseSome(
    dsts: Array[String])
  extends FrameworkMessage

private[varys] case class StartSome(
    dsts: Array[String])
  extends FrameworkMessage
