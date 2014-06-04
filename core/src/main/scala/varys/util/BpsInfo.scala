package varys.util

private[varys] class BpsInfo {

  // TODO: Might need a lock before updates and reads
  
  val HEARTBEAT_SEC = System.getProperty("varys.framework.heartbeat", "1").toInt
  val OLD_FACTOR = System.getProperty("varys.network.oldFactor", "0.2").toDouble
  val NIC_BPS = System.getProperty("varys.network.nicMbps", "1024").toDouble * 1024.0 * 1024.0

  var actualBps = 0.0
  var tempBps = 0.0
  var isTemp = false
  var lastUpdateTime = System.currentTimeMillis
  
  def resetToNormal(bps: Double) = { 
    this.actualBps = bps
    this.tempBps = bps
    this.isTemp = false
    this.lastUpdateTime = System.currentTimeMillis
  }
  
  def moveToTemp(blockSize: Long) = {
    // Into the temporary zone
    this.isTemp = true

    // Aim to increase by the remaining capacity of the link
    var incVal = NIC_BPS - this.tempBps
    if (incVal < 0.0) {
      incVal = 0.0
    }
    
    // Calculate the expected time till the next update
    val secElapsed = (System.currentTimeMillis - lastUpdateTime) / 1000.0
    val timeTillUpdate = 1.0 * HEARTBEAT_SEC - secElapsed

    // Bound incVal by blockSize
    if (timeTillUpdate > 0.0) {
      val temp = blockSize * 8.0 / timeTillUpdate
      if (temp < incVal) {
        incVal = temp
      }
    }
    
    this.tempBps += incVal
  }
  
  def update(curBps: Double) = {
    val newBps = (1.0 - OLD_FACTOR) * curBps + OLD_FACTOR * actualBps
    resetToNormal(newBps)
  }

  def getBps = if (isTemp) tempBps else actualBps
}
