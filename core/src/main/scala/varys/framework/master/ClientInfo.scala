package varys.framework.master

import java.util.Date
import akka.actor.ActorRef

private[varys] class ClientInfo(
    val startTime: Long,
    val id: String, 
    val host: String, 
    val submitDate: Date,
    val driver: ActorRef) 
{ 
  var endTime = -1L

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
}
