package varys.framework.slave

import akka.actor.{ActorRef, ActorSystem}
import akka.dispatch.Await
import akka.pattern.ask
import akka.util.Timeout
import akka.util.duration._
import cc.spray.Directives
import cc.spray.typeconversion.TwirlSupport._
import cc.spray.http.MediaTypes
import cc.spray.typeconversion.SprayJsonSupport._

import varys.framework.{SlaveState, RequestSlaveState}
import varys.framework.JsonProtocol._

/**
 * Web UI server for the standalone slave.
 */
private[varys] class SlaveWebUI(
    val actorSystem: ActorSystem, 
    slave: ActorRef) 
  extends Directives {
    
  val RESOURCE_DIR = "varys/framework/slave/webui"
  val STATIC_RESOURCE_DIR = "varys/framework/static"
  
  implicit val timeout = Timeout(10 seconds)
  
  val handler = {
    get {
      (path("") & parameters('format ?)) {
        case Some(js) if js.equalsIgnoreCase("json") => {
          val future = slave ? RequestSlaveState
          respondWithMediaType(MediaTypes.`application/json`) { ctx =>
            ctx.complete(future.mapTo[SlaveState])
          }
        }
        case _ =>
          completeWith{
            val future = slave ? RequestSlaveState
            future.map { slaveState =>
              varys.framework.slave.html.index(slaveState.asInstanceOf[SlaveState])
            }
          }
      } ~
      path("log") {
        parameters("coflowId", "executorId", "logType") { (coflowId, executorId, logType) =>
          respondWithMediaType(cc.spray.http.MediaTypes.`text/plain`) {
            getFromFileName("work/" + coflowId + "/" + executorId + "/" + logType)
          }
        }
      } ~
      pathPrefix("static") {
        getFromResourceDirectory(STATIC_RESOURCE_DIR)
      } ~
      getFromResourceDirectory(RESOURCE_DIR)
    }
  }
}
