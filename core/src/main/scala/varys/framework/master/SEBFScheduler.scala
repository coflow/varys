package varys.framework.master

import scala.collection.mutable.{ArrayBuffer, Map}

import varys.Logging

/**
 * Implementation of the Shortest-Effective-Bottleneck-First coflow scheduler.
 * It sorts coflows by expected CCT based on current bottlebneck duration.
 * All coflows are accepted; hence, there is no admission control.
 */

class SEBFScheduler extends OrderingBasedScheduler with Logging {

  val CONSIDER_DEADLINE = System.getProperty("varys.master.consdierDeadline", "false").toBoolean
  val DEADLINE_PADDING = System.getProperty("varys.master.deadlinePadding", "0.1").toDouble

  if (!CONSIDER_DEADLINE) {
    logError("varys.master.consdierDeadline must be true for DeadlineScheduler.")
    System.exit(1)
  }

  override def getOrderedCoflows(
      activeCoflows: ArrayBuffer[CoflowInfo]): ArrayBuffer[CoflowInfo] = {
    activeCoflows.sortWith(_.calcAlpha < _.calcAlpha)
  }

  override def calcFlowRate(
      flowInfo: FlowInfo,
      cf: CoflowInfo,
      minFree: Double): Double = {

    minFree * (flowInfo.getFlowSize() / cf.origAlpha)
  }
}
