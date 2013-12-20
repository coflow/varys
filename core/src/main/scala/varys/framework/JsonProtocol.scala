package varys.framework

import master.{ClientInfo, CoflowInfo, SlaveInfo}
import cc.spray.json._

/**
 * spray-json helper class containing implicit conversion to json for marshalling responses
 */
private[varys] object JsonProtocol extends DefaultJsonProtocol {
  implicit object SlaveInfoJsonFormat extends RootJsonWriter[SlaveInfo] {
    def write(obj: SlaveInfo) = JsObject(
      "id" -> JsString(obj.id),
      "host" -> JsString(obj.host),
      "webuiaddress" -> JsString(obj.webUiAddress)
    )
  }

  implicit object CoflowInfoJsonFormat extends RootJsonWriter[CoflowInfo] {
    def write(obj: CoflowInfo) = JsObject(
      "starttime" -> JsNumber(obj.startTime),
      "id" -> JsString(obj.id),
      "name" -> JsString(obj.desc.name),
      "user" -> JsString(obj.desc.user),
      "submitdate" -> JsString(obj.submitDate.toString))
  }

  implicit object ClientInfoJsonFormat extends RootJsonWriter[ClientInfo] {
    def write(obj: ClientInfo) = JsObject(
      "starttime" -> JsNumber(obj.startTime),
      "host" -> JsString(obj.host),
      "id" -> JsString(obj.id),
      "submitdate" -> JsString(obj.submitDate.toString))
  }

  implicit object CoflowDescriptionJsonFormat extends RootJsonWriter[CoflowDescription] {
    def write(obj: CoflowDescription) = JsObject(
      "name" -> JsString(obj.name),
      "user" -> JsString(obj.user)
    )
  }

  implicit object MasterStateJsonFormat extends RootJsonWriter[MasterState] {
    def write(obj: MasterState) = JsObject(
      "url" -> JsString("varys://" + obj.uri),
      "slaves" -> JsArray(obj.slaves.toList.map(_.toJson)),
      "activecoflows" -> JsArray(obj.activeCoflows.toList.map(_.toJson)),
      "completedcoflows" -> JsArray(obj.completedCoflows.toList.map(_.toJson)),
      "activeclients" -> JsArray(obj.activeClients.toList.map(_.toJson))
    )
  }

  implicit object SlaveStateJsonFormat extends RootJsonWriter[SlaveState] {
    def write(obj: SlaveState) = JsObject(
      "id" -> JsString(obj.slaveId),
      "masterurl" -> JsString(obj.masterUrl),
      "masterwebuiurl" -> JsString(obj.masterWebUiUrl),
      "rxbps" -> JsNumber(obj.rxBps),
      "txbps" -> JsNumber(obj.txBps)
    )
  }
}
