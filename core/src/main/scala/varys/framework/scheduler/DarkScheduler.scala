package varys.framework.scheduler

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.collection.JavaConversions._

import varys.framework.master.SlaveInfo
import varys.{Logging, Utils, VarysException}

private[framework] object DarkScheduler extends Logging {

  val MAX_DEPTH = System.getProperty("varys.framework.maxDagDepth", "100").toInt

  val NUM_JOB_QUEUES = System.getProperty("varys.framework.darkScheduler.numJobQueues", "10").toInt
  val INIT_QUEUE_LIMIT = 
    System.getProperty("varys.framework.darkScheduler.initQueueLimit", "10485760").toDouble
  val JOB_SIZE_MULT = System.getProperty("varys.framework.darkScheduler.jobSizeMult", "10").toDouble

  logTrace("varys.framework.darkScheduler.numJobQueues   = " + NUM_JOB_QUEUES)
  logTrace("varys.framework.darkScheduler.initQueueLimit = " + INIT_QUEUE_LIMIT)
  logTrace("varys.framework.darkScheduler.jobSizeMult    = " + JOB_SIZE_MULT)

  val allCoflows = new ConcurrentHashMap[Int, Coflow]()
  
  // Keep track of DAG-level coflow size (i.e., the sum of size of all coflows in a DAG)
  val dagLevelCoflowSizes = new HashMap[Int, Long]() { 
    override def default(key: Int) = 0L
  }
  
  // Keep track of coflows that are actually running at some slave
  val activeCoflows = new HashSet[Int]()

  private object sortedCoflowsLock
  val sortedCoflows = Array.ofDim[ArrayBuffer[Coflow]](NUM_JOB_QUEUES)
  for (i <- 0 until NUM_JOB_QUEUES) {
    sortedCoflows(i) = new ArrayBuffer[Coflow]()
  }

  /**
   * Add coflow to the end of the first queue
   */
  def addCoflow(coflowId: Int) {
    sortedCoflowsLock.synchronized {
      val cf = Coflow(coflowId)
      allCoflows(coflowId) = cf
      sortedCoflows(0) += cf
    }
  }

  def deleteCoflow(coflowId: Int) {
    val cf = allCoflows(coflowId)
    if (cf != null) {
      sortedCoflowsLock.synchronized {
        for (i <- 0 until NUM_JOB_QUEUES) {
          sortedCoflows(i) -= cf
        }
        if (allCoflows.containsValue(coflowId)) {
          allCoflows.remove(coflowId)
        }
      }
    }
  }

  def updateCoflowSizes(slaveInfos: ConcurrentHashMap[String, SlaveInfo]) {
    // Reset coflows that are active
    activeCoflows.clear

    sortedCoflowsLock.synchronized {
      // Reset all coflow-related stats
      dagLevelCoflowSizes.clear
      for ((_, cf) <- allCoflows) {
        cf.sizeSoFar = 0
        cf.flows.clear
      }  

      for ((slaveId, sInfo) <- slaveInfos) {
        for (i <- 0 until sInfo.numCoflows) {
          val cf = allCoflows(sInfo.coflowIds(i))
          if (cf != null) {
            dagLevelCoflowSizes(cf.coflowId / MAX_DEPTH) += sInfo.sizes(i)
            cf.sizeSoFar += sInfo.sizes(i)
            cf.flows += ((sInfo.id, sInfo.flows(i)))
          }

          // Remember active coflows
          activeCoflows += sInfo.coflowIds(i)        
        }
      }
    }
  }

  /**
   * Update coflow ordering based on currently known size
   */ 
  def updateCoflowOrder() {
    sortedCoflowsLock.synchronized {
      for (i <- 0 until NUM_JOB_QUEUES) {      
        sortedCoflows(i).clear
      }

      for ((cId, cf) <- allCoflows) {
        if (activeCoflows.contains(cId)) {
          val size = dagLevelCoflowSizes(cId / MAX_DEPTH)
          var curQ = 0
          var k = INIT_QUEUE_LIMIT
          while (k < size) {
            curQ += 1
            k *= JOB_SIZE_MULT
          }
          if (curQ >= NUM_JOB_QUEUES) {
            curQ = NUM_JOB_QUEUES - 1
          }
          cf.currentJobQueue = curQ
          sortedCoflows(curQ) += cf
        }
      }

      for (i <- 0 until NUM_JOB_QUEUES) {      
        sortedCoflows(i).sortBy(_.coflowId)
      }
    }
  }

  def getSchedule(slaveIds: Array[String]): (HashMap[String, HashSet[String]], Array[Int]) = { 
    val slaveAllocs = new HashMap[String, HashSet[String]]()
    for (id <- slaveIds) {
      slaveAllocs(id) = new HashSet[String]()
    }

    val NIC_Mbps = System.getProperty("varys.network.nicMbps", "1024").toDouble

    val srcFree = new HashMap[String, Double]() { 
      override def default(key: String) = NIC_Mbps 
    }
    val dstFree = new HashMap[String, Double]() { 
      override def default(key: String) = NIC_Mbps 
    }

    val retCoflows = new ArrayBuffer[Int]

    val numSrcFlows = new HashMap[String, Int]() { 
      override def default(key: String) = 0 
    }
    val numDstFlows = new HashMap[String, Int]() { 
      override def default(key: String) = 0
    }

    val srcUsed = new HashMap[String, Double]() { 
      override def default(key: String) = 0.0 
    }
    val dstUsed = new HashMap[String, Double]() { 
      override def default(key: String) = 0.0 
    }

    sortedCoflowsLock.synchronized {
      for (i <- 0 until NUM_JOB_QUEUES) {
        for (cf <- sortedCoflows(i)) {
          if (activeCoflows.contains(cf.coflowId)) {
            numSrcFlows.clear
            numDstFlows.clear

            for ((slaveId, dsts) <- cf.flows) {          
              if (srcFree(slaveId) > 0.0) {
                for (d <- dsts) {
                  if (dstFree(d) > 0.0) {
                    numSrcFlows(slaveId) += 1
                    numDstFlows(d) += 1
                  }
                }
              }
            }

            srcUsed.clear
            dstUsed.clear

            for ((slaveId, dsts) <- cf.flows) {          
              if (srcFree(slaveId) > 0.0) {
                for (d <- dsts) {
                  if (dstFree(d) > 0.0) {
                    var minFree = math.min(srcFree(slaveId) / numSrcFlows(slaveId),
                      dstFree(d) / numDstFlows(d))
                    if (minFree < 10.0) {
                      minFree = 0.0
                    }

                    srcUsed(slaveId) += minFree
                    dstUsed(d) += minFree

                    if (minFree > 0.0 && d != null) {
                      try {
                        slaveAllocs(slaveId) += d
                      } catch {
                        case e: Exception => {
                          logWarning("" + e)
                        }
                      }
                    }
                  }
                }
              }
            }

            for ((s, v) <- srcUsed) {
              srcFree(s) -= v
            }
            for ((d, v) <- dstUsed) {
              dstFree(d) -= v
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

    logDebug("%3d Sources".format(srcFree.size))
    logDebug("%3d Destinations".format(dstFree.size))

    (slaveAllocs, retCoflows.toArray)
  }

}
