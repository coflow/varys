package varys.framework.scheduler

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

import varys.{Logging, Utils, VarysException}

private[framework] object DarkScheduler extends Logging {

  val NUM_JOB_QUEUES = System.getProperty("varys.framework.darkScheduler.numJobQueues", "10").toInt
  val INIT_QUEUE_LIMIT = 
    System.getProperty("varys.framework.darkScheduler.initQueueLimit", "10485760").toDouble
  val JOB_SIZE_MULT = System.getProperty("varys.framework.darkScheduler.jobSizeMult", "10").toDouble

  val allCoflows = new ConcurrentHashMap[String, Coflow]()

  private object sortedCoflowsLock
  val sortedCoflows = Array.ofDim[ArrayBuffer[Coflow]](NUM_JOB_QUEUES)
  for (i <- 0 until NUM_JOB_QUEUES) {
    sortedCoflows(i) = new ArrayBuffer[Coflow]()
  }

  /**
   * Add coflow to the end of the first queue
   */
  def addCoflow(coflowId: String) {
    val cf = Coflow(coflowId)
    allCoflows(coflowId) = cf
    sortedCoflowsLock.synchronized {
      sortedCoflows(0) += cf
    }
  }

  def deleteCoflow(coflowId: String) {
    val cf = allCoflows(coflowId)
    if (cf != null) {
      sortedCoflowsLock.synchronized {
        for (i <- 0 until NUM_JOB_QUEUES) {
          sortedCoflows(i) -= cf
        }
      }
      if (allCoflows.containsValue(coflowId)) {
        allCoflows.remove(coflowId)
      }
    }
  }

  def updateCoflowSizes(coflowSizes: Array[(String, Long)]) {
    for ((coflowId, newSize) <- coflowSizes) {
      val cf = allCoflows(coflowId)
      if (cf != null) {
        cf.sizeSoFar = newSize
      }
    }
  }

  /**
   * Update coflow ordering based on currently known size
   */ 
  def updateCoflowOrder() {
    sortedCoflowsLock.synchronized {
      for (i <- 0 until NUM_JOB_QUEUES) {
        val coflowsToMove = new ArrayBuffer[Coflow]()
        for (cf <- sortedCoflows(i)) {
          val size = cf.sizeSoFar
          var curQ = 0
          var k = INIT_QUEUE_LIMIT
          while (k < size) {
            curQ += 1
            k *= JOB_SIZE_MULT
          }
          if (cf.currentJobQueue < curQ) {
            cf.currentJobQueue += 1
            coflowsToMove += cf
          }
        }
        if (i + 1 < NUM_JOB_QUEUES && coflowsToMove.size() > 0) {
          sortedCoflows(i) --= coflowsToMove
          sortedCoflows(i + 1) ++= coflowsToMove
        }
      }
    }
  }

  def getSchedule(): Array[(String, Long)] = {
    val retVal = new ArrayBuffer[(String, Long)]
    for (i <- 0 until NUM_JOB_QUEUES) {
      retVal ++= sortedCoflows(i).map(cf => (cf.coflowId, cf.sizeSoFar))
    }
    retVal.toArray
  }
}
