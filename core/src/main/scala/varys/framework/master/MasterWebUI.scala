package varys.framework.master

import akka.actor.{ActorRef, ActorSystem}
import akka.dispatch.Await
import akka.pattern.ask
import akka.util.Timeout
import akka.util.duration._
import cc.spray.Directives
import cc.spray.directives._
import cc.spray.typeconversion.TwirlSupport._
import cc.spray.http.MediaTypes
import cc.spray.typeconversion.SprayJsonSupport._

import varys.framework._
import varys.framework.JsonProtocol._

/**
 * Web UI server for the standalone master.
 */
private[varys]
class MasterWebUI(val actorSystem: ActorSystem, master: ActorRef) extends Directives {
  val RESOURCE_DIR = "varys/framework/master/webui"
  val STATIC_RESOURCE_DIR = "varys/framework/static"
  
  implicit val timeout = Timeout(10 seconds)
  
  val handler = {
    get {
      (path("") & parameters('format ?)) {
        case Some(js) if js.equalsIgnoreCase("json") =>
          val future = master ? RequestMasterState
          respondWithMediaType(MediaTypes.`application/json`) { ctx =>
            ctx.complete(future.mapTo[MasterState])
          }
        case _ =>
          completeWith {
            val future = master ? RequestMasterState
            future.map {
              masterState => varys.framework.master.html.index.render(masterState.asInstanceOf[MasterState])
            }
          }
      } ~
      path("job") {
        parameters("jobId", 'format ?) {
          case (jobId, Some(js)) if (js.equalsIgnoreCase("json")) =>
            val future = master ? RequestMasterState
            val jobInfo = for (masterState <- future.mapTo[MasterState]) yield {
              masterState.activeJobs.find(_.id == jobId).getOrElse({
                masterState.completedJobs.find(_.id == jobId).getOrElse(null)
              })
            }
            respondWithMediaType(MediaTypes.`application/json`) { ctx =>
              ctx.complete(jobInfo.mapTo[JobInfo])
            }
          case (jobId, _) =>
            completeWith {
              val future = master ? RequestMasterState
              future.map { state =>
                val masterState = state.asInstanceOf[MasterState]
                val job = masterState.activeJobs.find(_.id == jobId).getOrElse({
                  masterState.completedJobs.find(_.id == jobId).getOrElse(null)
                })
                varys.framework.master.html.job_details.render(job)
              }
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
