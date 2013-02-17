package varys.framework

import master.{JobInfo, SlaveInfo}
import cc.spray.json._

/**
 * spray-json helper class containing implicit conversion to json for marshalling responses
 */
private[varys] object JsonProtocol extends DefaultJsonProtocol {
  implicit object SlaveInfoJsonFormat extends RootJsonWriter[SlaveInfo] {
    def write(obj: SlaveInfo) = JsObject(
      "id" -> JsString(obj.id),
      "host" -> JsString(obj.host),
      "webuiaddress" -> JsString(obj.webUiAddress),
      "cores" -> JsNumber(obj.cores),
      "coresused" -> JsNumber(obj.coresUsed),
      "memory" -> JsNumber(obj.memory),
      "memoryused" -> JsNumber(obj.memoryUsed)
    )
  }

  implicit object JobInfoJsonFormat extends RootJsonWriter[JobInfo] {
    def write(obj: JobInfo) = JsObject(
      "starttime" -> JsNumber(obj.startTime),
      "id" -> JsString(obj.id),
      "name" -> JsString(obj.desc.name),
      "cores" -> JsNumber(obj.desc.cores),
      "user" -> JsString(obj.desc.user),
      "memoryperslave" -> JsNumber(obj.desc.memoryPerSlave),
      "submitdate" -> JsString(obj.submitDate.toString))
  }

  implicit object JobDescriptionJsonFormat extends RootJsonWriter[JobDescription] {
    def write(obj: JobDescription) = JsObject(
      "name" -> JsString(obj.name),
      "cores" -> JsNumber(obj.cores),
      "memoryperslave" -> JsNumber(obj.memoryPerSlave),
      "user" -> JsString(obj.user)
    )
  }

  implicit object MasterStateJsonFormat extends RootJsonWriter[MasterState] {
    def write(obj: MasterState) = JsObject(
      "url" -> JsString("varys://" + obj.uri),
      "slaves" -> JsArray(obj.slaves.toList.map(_.toJson)),
      "cores" -> JsNumber(obj.slaves.map(_.cores).sum),
      "coresused" -> JsNumber(obj.slaves.map(_.coresUsed).sum),
      "memory" -> JsNumber(obj.slaves.map(_.memory).sum),
      "memoryused" -> JsNumber(obj.slaves.map(_.memoryUsed).sum),
      "activejobs" -> JsArray(obj.activeJobs.toList.map(_.toJson)),
      "completedjobs" -> JsArray(obj.completedJobs.toList.map(_.toJson))
    )
  }

  implicit object SlaveStateJsonFormat extends RootJsonWriter[SlaveState] {
    def write(obj: SlaveState) = JsObject(
      "id" -> JsString(obj.slaveId),
      "masterurl" -> JsString(obj.masterUrl),
      "masterwebuiurl" -> JsString(obj.masterWebUiUrl),
      "cores" -> JsNumber(obj.cores),
      "coresused" -> JsNumber(obj.coresUsed),
      "memory" -> JsNumber(obj.memory),
      "memoryused" -> JsNumber(obj.memoryUsed)
    )
  }
}
