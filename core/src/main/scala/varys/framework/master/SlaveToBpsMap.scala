package varys.framework.master

import scala.collection.mutable.{ArrayBuffer, SynchronizedMap, HashMap, SynchronizedSet, HashSet}
import scala.util.Random

private[varys] class SlaveToBpsMap {
  
  val OLD_FACTOR = System.getProperty("varys.network.oldFactor", "0.2").toDouble
  
  val writeBlockRanGen = new Random()
  
  val idToBpsMap = new HashMap[String, BpsInfo] with SynchronizedMap[String, BpsInfo]
  
  def size = idToBpsMap.size
  def keys = idToBpsMap.keys
  def values = idToBpsMap.values
  
  def updateNetworkInfo(id: String, newBps: Double) = {
    val bpsInfo = idToBpsMap.getOrElse(id, new BpsInfo())
    val bps = (1.0 - OLD_FACTOR) * newBps + OLD_FACTOR * bpsInfo.bps
    bpsInfo.resetToNormal(bps)
    idToBpsMap.put(id, bpsInfo)
  }

  def adjustBps(id: String, blockSize: Long) = {
    val bpsInfo = idToBpsMap.getOrElse(id, new BpsInfo())
    bpsInfo.moveToTemp(blockSize)
    idToBpsMap.put(id, bpsInfo)
  }

  def getBps(id: String): Double = {
    val bpsInfo = idToBpsMap.getOrElse(id, new BpsInfo())
    if (bpsInfo.isTemp) bpsInfo.tempBps else bpsInfo.bps
  }

  def getRandom(adjustBytes: Long): String = this.synchronized {
    val ret = getRandomN(1, adjustBytes)
    if (ret == null || ret.size == 0) null else ret(0)
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
  
  def getTop(adjustBytes: Long): String = this.synchronized {
    val ret = getTopN(1, adjustBytes)
    if (ret == null || ret.size == 0) null else ret(0)
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
