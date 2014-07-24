package varys.util

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit._

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.collection.JavaConversions._

import varys.{Logging, Utils, VarysException}

private[varys] class IPTablesClient(clientName: String) extends Logging {

  val IPTABLES_UPDATE_FREQ = 40 // milliseconds
  val MAX_RULES_IN_MULTIPORT = 15

  case class CoflowInfo(
    val coflowId: String,
    var curSize: Long,
    val flows: HashSet[Int]
  )

  private val coflows = new ConcurrentHashMap[String, CoflowInfo]
  private var tableUpdated = false

  // Periodically update iptables if anything has changed
  private val scheduler = Executors.newScheduledThreadPool(1)
  private val updateTask = new Runnable() {
    def run() { 
      if (tableUpdated) {
        updateIPTables
        tableUpdated = false
      }
    }
  }
  private val stopHandle = scheduler.scheduleAtFixedRate(updateTask, 0, IPTABLES_UPDATE_FREQ, MILLISECONDS)

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
    val outputFile = "/tmp/VarysIPTables-%s.txt".format(clientName)
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
    val theTable = Utils.runShellCommand(
      "iptables -vL INPUT; iptables-restore %s".format(outputFile))
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
    stopHandle.cancel(true)
  }

  def addCoflow(coflowId: String) {
    if (coflows.containsKey(coflowId)) {
      throw new VarysException("%s already exists!".format(coflowId))
    }
    coflows(coflowId) = CoflowInfo(coflowId, 0, new HashSet[Int])
    tableUpdated = true
  }

  def deleteCoflow(coflowId: String) {
    if (!coflows.containsKey(coflowId)) {
      throw new VarysException("%s doesn't exist!".format(coflowId))
    }
    coflows -= coflowId
    tableUpdated = true
  }

  def addFlow(coflowId: String, srcPort: Int) {
    if (!coflows.containsKey(coflowId)) {
      throw new VarysException("%s doesn't exist!".format(coflowId))
    }
    coflows(coflowId).flows += srcPort
    tableUpdated = true
  }

  def deleteFlow(coflowId: String, srcPort: Int) {
    if (!coflows.containsKey(coflowId)) {
      throw new VarysException("%s doesn't exist!".format(coflowId))
    }
    coflows(coflowId).flows -= srcPort
    tableUpdated = true
  }

  def addFlows(coflowId: String, srcPorts: Array[Int]) {
    if (!coflows.containsKey(coflowId)) {
      throw new VarysException("%s doesn't exist!".format(coflowId))
    }
    coflows(coflowId).flows ++= srcPorts
    tableUpdated = true
  }

  def deleteFlows(coflowId: String, srcPorts: Array[Int]) {
    if (!coflows.containsKey(coflowId)) {
      throw new VarysException("%s doesn't exist!".format(coflowId))
    }
    coflows(coflowId).flows --= srcPorts
    tableUpdated = true
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

}

private[varys] object IPTablesClient {
  def main(args: Array[String]) {
    val iptables = new IPTablesClient("Test")
    for (i <- 0 to 10) {
      iptables.addCoflow("COFLOW-" + i)
      var r = new scala.util.Random
      for (j <- 0 to r.nextInt(20)) {
        iptables.addFlow("COFLOW-" + i, (i+1) * 1000 + j)
      }
    }
    iptables.updateIPTables()
    Thread.sleep(1000)
    iptables.stop()
    System.exit(1)
  }  
}
