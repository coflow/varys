package varys.framework

import net.liftweb.json.JsonDSL._

import varys.framework.master.{CoflowInfo, SlaveInfo}

private[varys] object JsonProtocol {
  def writeSlaveInfo(obj: SlaveInfo) = {
    ("id" -> obj.id) ~
    ("host" -> obj.host) ~
    ("port" -> obj.port) ~
    ("webuiaddress" -> obj.webUiAddress)
  }

  def writeCoflowInfo(obj: CoflowInfo) = {
    ("starttime" -> obj.startTime) ~
    ("id" -> obj.id) ~
    ("name" -> obj.desc.name) ~
    ("user" ->  obj.desc.user) ~
    ("submitdate" -> obj.submitDate.toString)
    ("state" -> obj.curState.toString) ~
    ("duration" -> obj.duration)
  }

  def writeClientInfo(obj: ClientInfo) = {
    ("starttime" -> obj.startTime) ~
    ("host" -> obj.host) ~
    ("id" -> obj.id) ~
    ("user" ->  obj.user) ~
    ("submitdate" -> obj.submitDate.toString)
  }

  def writeCoflowDescription(obj: CoflowDescription) = {
    ("name" -> obj.name) ~
    ("user" -> obj.user)
  }

  def writeMasterState(obj: MasterState) = {
    ("url" -> ("varys://" + obj.uri)) ~
    ("slaves" -> obj.slaves.toList.map(writeSlaveInfo)) ~
    ("activecoflows" -> obj.activeCoflows.toList.map(writeCoflowInfo)) ~
    ("completedcoflows" -> obj.completedCoflows.toList.map(writeCoflowInfo)) ~
    ("activeclients" -> obj.activeClients.toList.map(writeClientInfo))
  }

  def writeSlaveState(obj: SlaveState) = {
    ("id" -> obj.slaveId) ~
    ("masterurl" -> obj.masterUrl) ~
    ("masterwebuiurl" -> obj.masterWebUiUrl) ~
    ("rxbps" -> obj.rxBps) ~
    ("txbps" -> obj.txBps)
  }
}
