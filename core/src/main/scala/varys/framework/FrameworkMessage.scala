package varys.framework

import akka.actor.ActorRef

import varys.framework.master.{CoflowInfo, ClientInfo, SlaveInfo}

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

// Master to Slave
private[varys] case class RegisteredSlave(
    masterWebUiUrl: String) 
  extends FrameworkMessage

private[varys] case class RegisterSlaveFailed(
    message: String) 
  extends FrameworkMessage

// Client to Master
private[varys] case class RegisterClient(
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

// Master/Client to Client/Slave
private[varys] case class RegisteredClient(
    clientId: String, 
    slaveId: String,
    slaveUrl:String) 
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

private[varys] case class UpdatedRates(
    newRates: Map[DataIdentifier, Double]) 
  extends FrameworkMessage

// Client/Slave to Slave/Master
private[varys] case class AddFlow(
    flowDescription: FlowDescription) 
  extends FrameworkMessage

private[varys] case class AddFlows(
    flowDescriptions: Array[FlowDescription], 
    coflowId: String, 
    dataType: DataType.DataType) 
  extends FrameworkMessage

private[varys] case class GetFlow(
    flowId: String, 
    coflowId: String, 
    clientId: String,
    slaveId: String,
    flowDesc: FlowDescription = null) 
  extends FrameworkMessage

private[varys] case class GetFlows(
    flowIds: Array[String], 
    coflowId: String, 
    clientId: String,
    slaveId: String,
    flowDescs: Array[FlowDescription] = null) 
  extends FrameworkMessage

private[varys] case class FlowProgress(
    flowId: String, 
    coflowId: String, 
    bytesSinceLastUpdate: Long, 
    isCompleted: Boolean)
  extends FrameworkMessage

private[varys] case class DeleteFlow(
    flowId: String, 
    coflowId: String) 
  extends FrameworkMessage

// Slave/Master to Client/Slave
private[varys] case class GotFlowDesc(
    flowDesc: FlowDescription) 
  extends FrameworkMessage

private[varys] case class GotFlowDescs(
    flowDescs: Array[FlowDescription]) 
  extends FrameworkMessage

// Internal message in Client/Slave
private[varys] case object StopClient

private[varys] case class GetRequest(
    flowDesc: FlowDescription, 
    targetHost: String = null,
    targetCommPort: Int = 0) {
    
  // Not extending FrameworkMessage because it is NOT going through akka serialization
  // override def toString: String = "GetRequest(" + flowDesc.id+ ":" + flowDesc.coflowId + ")"
} 

// Internal message in Master
private[varys] case object ScheduleRequest

private[varys] case object CheckForSlaveTimeOut

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
