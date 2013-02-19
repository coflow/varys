package varys.framework.master

import varys.framework.CoflowDescription
import java.util.Date
import akka.actor.ActorRef
import scala.collection.mutable

private[varys] class CoflowInfo(
    val startTime: Long,
    val id: String,
    val desc: CoflowDescription,
    val submitDate: Date,
    val driver: ActorRef)
{
  var state = CoflowState.WAITING
  var coresGranted = 0
  var endTime = -1L

  def coresLeft: Int = desc.cores - coresGranted

  private var _retryCount = 0

  def retryCount = _retryCount

  def incrementRetryCount = {
    _retryCount += 1
    _retryCount
  }

  def markFinished(endState: CoflowState.Value) {
    state = endState
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
