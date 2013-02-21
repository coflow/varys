package varys.framework

import varys.framework.master.{SlaveInfo, CoflowInfo}
import scala.collection.immutable.List

private[varys] sealed trait FrameworkMessage extends Serializable

// Slave to Master
private[varys]
case class RegisterSlave(
    id: String,
    host: String,
    port: Int,
    cores: Int,
    webUiPort: Int,
    publicAddress: String)
  extends FrameworkMessage
private[varys]
case class Heartbeat(
    slaveId: String,
    rxBps: Double,
    txBps: Double)
  extends FrameworkMessage

// Master to Slave
private[varys] case class RegisteredSlave(masterWebUiUrl: String) extends FrameworkMessage
private[varys] case class RegisterSlaveFailed(message: String) extends FrameworkMessage

// Client to Master
private[varys] case class RegisterCoflow(coflowDescription: CoflowDescription) extends FrameworkMessage
private[varys] case class RequestBestRxMachines(howMany: Int, adjustBytes: Long) extends FrameworkMessage
private[varys] case class RequestBestTxMachines(howMany: Int, adjustBytes: Long) extends FrameworkMessage

// Master to Client
private[varys] case class RegisteredCoflow(coflowId: String) extends FrameworkMessage
private[varys] case class CoflowKilled(message: String)
private[varys] case class BestRxMachines(bestRxMachines: Array[String]) extends FrameworkMessage
private[varys] case class BestTxMachines(bestTxMachines: Array[String]) extends FrameworkMessage

// Internal message in Client
private[varys] case object StopClient

// MasterWebUI To Master
private[varys] case object RequestMasterState

// Master to MasterWebUI
private[varys] 
case class MasterState(
    host: String, 
    port: Int, 
    slaves: Array[SlaveInfo],
    activeCoflows: Array[CoflowInfo], 
    completedCoflows: Array[CoflowInfo]) {

  def uri = "varys://" + host + ":" + port
}

//  SlaveWebUI to Slave
private[varys] case object RequestSlaveState

// Slave to SlaveWebUI
private[varys]
case class SlaveState(
    host: String, 
    port: Int, 
    slaveId: String, 
    masterUrl: String, 
    cores: Int, 
    coresUsed: Int, 
    rxBps: Double,
    txBps: Double,
    masterWebUiUrl: String)
