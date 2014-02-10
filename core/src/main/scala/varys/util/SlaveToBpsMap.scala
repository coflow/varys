package varys.util

import scala.collection.mutable.{ArrayBuffer, SynchronizedMap, HashMap}
import scala.util.Random

private[varys] class SlaveToBpsMap {
    
  val writeBlockRanGen = new Random()
  
  val idToBpsMap = new HashMap[String, BpsInfo] with SynchronizedMap[String, BpsInfo]
  
  def updateNetworkStats(id: String, newBps: Double) = {
    val bpsInfo = idToBpsMap.getOrElse(id, new BpsInfo())
    bpsInfo.update(newBps)
    idToBpsMap.put(id, bpsInfo)
  }

  def adjustBps(id: String, blockSize: Long) = {
    val bpsInfo = idToBpsMap.getOrElse(id, new BpsInfo())
    bpsInfo.moveToTemp(blockSize)
    idToBpsMap.put(id, bpsInfo)
  }

  def getBps(id: String): Double = {
    val bpsInfo = idToBpsMap.getOrElse(id, new BpsInfo())
    bpsInfo.getBps
  }

  def getRandomN(numMachines: Int, adjustBytes: Long): ArrayBuffer[String] = this.synchronized {
    val retVal = new ArrayBuffer[String]
    val machines = idToBpsMap.keys.toList
    assert(numMachines <= machines.size)
    
    val wasSelected = Array.ofDim[Boolean](machines.size)
    var machinesToPick = numMachines
    while (machinesToPick > 0) {
      machinesToPick -= 1
      var toAdd = -1
      while (toAdd == -1) {
        toAdd = writeBlockRanGen.nextInt(machines.size)
        if (wasSelected(toAdd)) {
          toAdd = -1
        }
      }
      retVal += machines(toAdd)
      adjustBps(machines(toAdd), adjustBytes)
      wasSelected(toAdd) = true
    }
    retVal
  }
  
  def getTopN(numMachines: Int, adjustBytes: Long): ArrayBuffer[String] = this.synchronized {
    val retVal = new ArrayBuffer[String]
    val machines = idToBpsMap.keys.toList
    assert(numMachines <= machines.size)

    def compByBps(o1: String, o2: String) = {
      val o1bps = getBps(o1)
      val o2bps = getBps(o2)
      if (o1bps < o2bps) true else false
    }
    machines.sortWith(compByBps)
    
    for (i <- 0 until numMachines) {
      retVal += machines(i)
      adjustBps(machines(i), adjustBytes)
    }
    retVal
  }
}
