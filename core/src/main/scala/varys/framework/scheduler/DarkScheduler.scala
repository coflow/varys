package varys.framework.scheduler

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.collection.JavaConversions._

import varys.framework.master.SlaveInfo
import varys.{Logging, Utils, VarysException}

private[framework] object DarkScheduler extends Logging {

  val NUM_JOB_QUEUES = System.getProperty("varys.framework.darkScheduler.numJobQueues", "10").toInt
  val INIT_QUEUE_LIMIT = 
    System.getProperty("varys.framework.darkScheduler.initQueueLimit", "10485760").toDouble
  val JOB_SIZE_MULT = System.getProperty("varys.framework.darkScheduler.jobSizeMult", "10").toDouble

  logTrace("varys.framework.darkScheduler.numJobQueues   = " + NUM_JOB_QUEUES)
  logTrace("varys.framework.darkScheduler.initQueueLimit = " + INIT_QUEUE_LIMIT)
  logTrace("varys.framework.darkScheduler.jobSizeMult    = " + JOB_SIZE_MULT)

  val allCoflows = new ConcurrentHashMap[String, Coflow]()
  
  // Keep track of coflows that are actually running at some slave
  val activeCoflows = new HashSet[String]()

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

  def updateCoflowSizes(slaveInfos: ConcurrentHashMap[String, SlaveInfo]) {
    // Reset coflows that are active
    activeCoflows.clear

    // Reset all coflow-related stats except for their locations in the job queue
    for ((_, cf) <- allCoflows) {
      cf.sizeSoFar = 0
      cf.flows.clear
    }

    for ((slaveId, sInfo) <- slaveInfos) {
      for (i <- 0 until sInfo.numCoflows) {
        val cf = allCoflows(sInfo.coflowIds(i))
        if (cf != null) {
          cf.sizeSoFar += sInfo.sizes(i)
          cf.flows += ((sInfo.id, sInfo.flows(i)))
        }

        // Remember active coflows
        activeCoflows += sInfo.coflowIds(i)        
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

  def getSchedule(slaveIds: Array[String]): (HashMap[String, ArrayBuffer[String]], Array[String]) = {
    
    val slaveAllocs = new HashMap[String, ArrayBuffer[String]]()
    for (id <- slaveIds) {
      slaveAllocs(id) = new ArrayBuffer[String]()
    }

    val srcUsedBy = new HashMap[String, String]() { 
      override def default(key: String) = null 
    }
    val dstUsedBy = new HashMap[String, String]() { 
      override def default(key: String) = null 
    }

    val retCoflows = new ArrayBuffer[String]

    sortedCoflowsLock.synchronized {
      for (i <- 0 until NUM_JOB_QUEUES) {
        for (cf <- sortedCoflows(i)) {
          if (activeCoflows.contains(cf.coflowId)) {
            for ((slaveId, dsts) <- cf.flows) {          
              if (srcUsedBy(slaveId) == null || srcUsedBy(slaveId) == cf.coflowId) {
                var srcInUse = false
                for (d <- dsts) {
                  if (dstUsedBy(d) == null || dstUsedBy(d) == cf.coflowId) {
                    dstUsedBy(d) = cf.coflowId
                    slaveAllocs(slaveId) += d
                    srcInUse = true
                  }
                }
                if (srcInUse) {
                  srcUsedBy(slaveId) = cf.coflowId
                }
              }
            }
          }
        }
      }

      for (i <- 0 until NUM_JOB_QUEUES) {
        for (cf <- sortedCoflows(i)) {
          if (activeCoflows.contains(cf.coflowId)) {
            retCoflows += cf.coflowId
          }
        }
      }
    }

    (slaveAllocs, retCoflows.toArray)
  }
}
