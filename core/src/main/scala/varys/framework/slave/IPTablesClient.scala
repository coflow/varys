package varys.framework.slave

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit._

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.collection.JavaConversions._

import varys.{Logging, Utils, VarysException}

private[slave] object IPTablesClient extends Logging {

  case class CoflowInfo(
    val coflowId: String,
    var curSize: Long,
    val flows: HashSet[Int],
    var lastSweeped: Long = -1
  )

  private val coflows = new ConcurrentHashMap[String, CoflowInfo]
  private var tableUpdated = false

  // Periodically drop old coflows
  private val CLEANUP_INTERVAL_MS = System.getProperty("varys.slave.iptcCleanupSec", "120").toInt * 1000
  private val cleanupScheduler = Executors.newScheduledThreadPool(1)
  private val cleanupTask = new Runnable() {
    def run() { 
      logTrace("Cleaning up dead coflows")
      val allCoflows = coflows.values.toBuffer.asInstanceOf[ArrayBuffer[CoflowInfo]]
      val toRemove = allCoflows.filter(x => 
        (System.currentTimeMillis - x.lastSweeped) >= CLEANUP_INTERVAL_MS)
      val numToRemove = toRemove.size
      toRemove.foreach(c => coflows -= c.coflowId)
      allCoflows.foreach(c => {
        if (!toRemove.contains(c.coflowId)) {
          c.lastSweeped = System.currentTimeMillis
        }
      })
      logTrace("Removed %d dead coflows %s".format(numToRemove, toRemove))
    }
  }
  private val cleanupStopHandle = cleanupScheduler.scheduleAtFixedRate(cleanupTask, 
    CLEANUP_INTERVAL_MS, CLEANUP_INTERVAL_MS, MILLISECONDS)

  // Periodically update iptables if anything has changed
  private val IPTABLES_UPDATE_FREQ_MS = System.getProperty("varys.slave.iptcUpdateMillis", "40").toInt
  private val MAX_RULES_IN_MULTIPORT = 15

  private val updateScheduler = Executors.newScheduledThreadPool(1)
  private val updateTask = new Runnable() {
    def run() { 
      if (tableUpdated) {
        logTrace("Updating iptables")
        updateIPTables
        tableUpdated = false
      }
    }
  }
  private val updateStopHandle = updateScheduler.scheduleAtFixedRate(updateTask, 0, 
    IPTABLES_UPDATE_FREQ_MS, MILLISECONDS)

  private def updateIPTables() {
    val toWrite = new ArrayBuffer[String]
    
    toWrite += "*filter"
    toWrite += ":INPUT ACCEPT [0:0]"
    toWrite += ":FORWARD ACCEPT [0:0]"
    toWrite += ":OUTPUT ACCEPT [0:0]"
    for ((coflowId, _) <- coflows) {
      toWrite += ":%s - [0:0]".format(coflowId)
    }
    for ((coflowId, coflowInfo) <- coflows) {
      val flows = coflowInfo.flows.toArray
      val numFlows = flows.length
      for (i <- 0 to (numFlows - 1) / MAX_RULES_IN_MULTIPORT) {
        var aLine = "-A INPUT -p tcp -m multiport --dports "
        var j = 0
        while (i * MAX_RULES_IN_MULTIPORT + j < numFlows && j < MAX_RULES_IN_MULTIPORT) {
          if (j > 0) {
            aLine += ","
          }
          aLine += flows(i * MAX_RULES_IN_MULTIPORT + j)
          j += 1
        }
        aLine += " -j %s".format(coflowId)
        toWrite += aLine
      }
      toWrite += "-A %s -j ACCEPT".format(coflowId)
    }
    toWrite += "COMMIT"
    val outputFile = "/tmp/VarysIPTables.txt"
    Utils.writeToFile(toWrite.toArray, outputFile)

    // Remember old sizes and update iptables
    val curSizes = getCoflowSizesAndUpdate(outputFile)
    for ((cf, size) <- curSizes) {
      coflows(cf).curSize += size
    }
  }

  /**
   * Calculate bytes transferred since last update and update iptables
   * This saves one ProcessBuilder creation (appox. 10 milliseconds)
   */ 
  private def getCoflowSizesAndUpdate(outputFile: String): HashMap[String, Long] = {
    val retVal = new HashMap[String, Long]

    // Not very clean, but we can run two commands in one ProcessBuilder call. Ugly.
    val theTable = Utils.runShellCommand(
      "iptables -vL INPUT; iptables-restore %s".format(outputFile))
    
    // Ignore first two lines
    var a = 2
    while (a < theTable.length) { 
      val pieces = theTable(a).split("\\s+")

      var toAdd: Long = 0
      if (coflows.containsKey(pieces(3))) {
        toAdd = coflows(pieces(3)).curSize
      }

      retVal(pieces(3)) = pieces(2).toLong + toAdd
      a += 1
    }
    retVal
  }

  def stop() {
    cleanupStopHandle.cancel(true)
    updateStopHandle.cancel(true)
  }

  def addCoflow(coflowId: String) {
    if (coflows.containsKey(coflowId)) {
      logWarning("Coflow %s already exists! Ignoring...".format(coflowId))
    } else {
      coflows(coflowId) = CoflowInfo(coflowId, 0, new HashSet[Int])
      tableUpdated = true
    }
  }

  def deleteCoflow(coflowId: String) {
    if (!coflows.containsKey(coflowId)) {
      logWarning("Coflow %s doesn't exist!".format(coflowId))
    } else {
      coflows -= coflowId
      tableUpdated = true
    }
  }

  def addFlow(coflowId: String, dstPort: Int) {
    if (!coflows.containsKey(coflowId)) {
      logWarning("Coflow %s doesn't exist! Trying to create...".format(coflowId))
      addCoflow(coflowId)
    }
    coflows(coflowId).flows += dstPort
    tableUpdated = true
  }

  def deleteFlow(coflowId: String, dstPort: Int) {
    if (!coflows.containsKey(coflowId)) {
      logWarning("Coflow %s doesn't exist! Ignoring...".format(coflowId))
    } else {
      coflows(coflowId).flows -= dstPort
      tableUpdated = true
    }
  }

  def addFlows(coflowId: String, dstPorts: Array[Int]) {
    if (!coflows.containsKey(coflowId)) {
      logWarning("Coflow %s doesn't exist! Trying to create...".format(coflowId))
      addCoflow(coflowId)
    }
    coflows(coflowId).flows ++= dstPorts
    tableUpdated = true
  }

  def deleteFlows(coflowId: String, dstPorts: Array[Int]) {
    if (!coflows.containsKey(coflowId)) {
      logWarning("Coflow %s doesn't exist! Ignoring...".format(coflowId))
    } else {
      coflows(coflowId).flows --= dstPorts
      tableUpdated = true
    }
  }

  /**
   * Returns the actual size of each active coflow
   */ 
  def getActiveCoflowSizes(): HashMap[String, Long] = {
    val retVal = new HashMap[String, Long]
    val theTable = Utils.runShellCommand("iptables -vL INPUT")
    var a = 2
    while (a < theTable.length) { 
      val pieces = theTable(a).split("\\s+")

      var toAdd: Long = 0
      if (coflows.containsKey(pieces(3))) {
        toAdd = coflows(pieces(3)).curSize
      }

      retVal(pieces(3)) = pieces(2).toLong + toAdd
      a += 1
    }
    retVal
  }

  def main(args: Array[String]) {
    for (i <- 0 to 10) {
      IPTablesClient.addCoflow("COFLOW-" + i)
      var r = new scala.util.Random
      for (j <- 0 to r.nextInt(20)) {
        IPTablesClient.addFlow("COFLOW-" + i, (i+1) * 1000 + j)
      }
    }
    IPTablesClient.updateIPTables()
    Thread.sleep(1000)
    IPTablesClient.stop()
    System.exit(1)
  }  

}
