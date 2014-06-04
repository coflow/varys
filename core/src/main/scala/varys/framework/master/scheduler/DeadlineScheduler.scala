package varys.framework.master.scheduler

import scala.collection.mutable.{ArrayBuffer, Map}

import varys.framework.master.{CoflowInfo, CoflowState, FlowInfo, SlaveInfo}
import varys.Logging

/**
 * Implementation of a deadline-based coflow scheduler with admission control. 
 */
class DeadlineScheduler extends OrderingBasedScheduler with Logging {

  val CONSIDER_DEADLINE = System.getProperty("varys.master.consdierDeadline", "false").toBoolean
  val DEADLINE_PAD = System.getProperty("varys.master.deadlinePadding", "0.1").toDouble
  val MIN_DEADLINE = System.getProperty("varys.master.minDeadlineMillis", "200").toInt

  if (!CONSIDER_DEADLINE) {
    logError("varys.master.consdierDeadline must be true for DeadlineScheduler")
    System.exit(1)
  }

  override def getOrderedCoflows(
      activeCoflows: ArrayBuffer[CoflowInfo]): ArrayBuffer[CoflowInfo] = {
    activeCoflows.sortWith(_.readyTime < _.readyTime)
  }

  override def markForRejection(
      cf: CoflowInfo, 
      sBpsFree: Map[String, Double], 
      rBpsFree: Map[String, Double]): Boolean = {
    
    val minMillis = math.max(cf.calcRemainingMillis(sBpsFree, rBpsFree) * (1 + DEADLINE_PAD), MIN_DEADLINE)
    
    val rejected = (cf.curState == CoflowState.READY && minMillis > cf.desc.deadlineMillis)
    if (rejected) {
      val rejectMessage = "Minimum completion time of " + minMillis + 
        " millis is more than the deadline of " + cf.desc.deadlineMillis + " millis"
      logInfo("Marking " + cf + " for rejection => " + rejectMessage)
    }

    rejected
  }

  override def calcFlowRate(
      flowInfo: FlowInfo,
      cf: CoflowInfo,
      minFree: Double): Double = {

    math.min((flowInfo.bytesLeft.toDouble * 8) / (cf.desc.deadlineMillis.toDouble / 1000), minFree)
  }
}
