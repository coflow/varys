package varys.framework.master

import akka.actor.ActorRef

import java.util.Date

import scala.collection.mutable.{HashSet}

private[varys] class ClientInfo(
    val startTime: Long,
    val id: String, 
    val host: String, 
    val commPort: Int,
    val submitDate: Date,
    val actor: ActorRef) { 
  
  var endTime = -1L
  var coflows = new HashSet[CoflowInfo]

  def markFinished() {
    endTime = System.currentTimeMillis()
  }
  
  def duration: Long = {
    if (endTime != -1) {
      endTime - startTime
    } else {
      System.currentTimeMillis() - startTime
    }
  }
  
  def addCoflow(coflow: CoflowInfo) {
    coflows += coflow
  }
  
  val user = System.getProperty("user.name", "<unknown>")
  
  override def toString: String = "ClientInfo(" + id + "[" + host + ":" + commPort + "])"
}
