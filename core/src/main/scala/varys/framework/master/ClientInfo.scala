package varys.framework.master

import java.util.Date
import akka.actor.ActorRef
import scala.collection.mutable.{HashSet}

private[varys] class ClientInfo(
    val startTime: Long,
    val id: String, 
    val host: String, 
    val commPort: Int,
    val submitDate: Date,
    val actor: ActorRef) 
{ 
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
  
  override def toString: String = "SlaveInfo(" + id + "[" + host + ":" + commPort + "])"
}
