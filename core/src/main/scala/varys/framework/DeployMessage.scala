package varys.framework

import varys.framework.master.{SlaveInfo, JobInfo}
import scala.collection.immutable.List


private[varys] sealed trait FrameworkMessage extends Serializable

// Slave to Master

private[varys]
case class RegisterSlave(
    id: String,
    host: String,
    port: Int,
    cores: Int,
    memory: Int,
    webUiPort: Int,
    publicAddress: String)
  extends FrameworkMessage

private[varys] case class Heartbeat(slaveId: String) extends FrameworkMessage

// Master to Slave

private[varys] case class RegisteredSlave(masterWebUiUrl: String) extends FrameworkMessage
private[varys] case class RegisterSlaveFailed(message: String) extends FrameworkMessage

// Client to Master

private[varys] case class RegisterJob(jobDescription: JobDescription) extends FrameworkMessage

// Master to Client

private[varys] 
case class RegisteredJob(jobId: String) extends FrameworkMessage

private[varys]
case class JobKilled(message: String)

// Internal message in Client

private[varys] case object StopClient

// MasterWebUI To Master

private[varys] case object RequestMasterState

// Master to MasterWebUI

private[varys] 
case class MasterState(host: String, port: Int, slaves: Array[SlaveInfo],
  activeJobs: Array[JobInfo], completedJobs: Array[JobInfo]) {

  def uri = "varys://" + host + ":" + port
}

//  SlaveWebUI to Slave
private[varys] case object RequestSlaveState

// Slave to SlaveWebUI

private[varys]
case class SlaveState(host: String, port: Int, slaveId: String, masterUrl: String, cores: Int, memory: Int, 
  coresUsed: Int, memoryUsed: Int, masterWebUiUrl: String)
