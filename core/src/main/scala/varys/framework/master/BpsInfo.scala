package varys.framework.master

import varys.VarysCommon

private[varys] class BpsInfo {
  var bps = 0.0
  var tempBps = 0.0
  var isTemp = false
  var lastUpdateTime = System.currentTimeMillis
  
  def resetToNormal(bps: Double) = {  
    this.bps = bps
    this.tempBps = bps
    this.isTemp = false
    this.lastUpdateTime = System.currentTimeMillis
  }
  
  def moveToTemp(blockSize: Long) = {
    // Into the temporary zone
    this.isTemp = true

    // 1Gbps == 128MBps
    val nicSpeed = 128.0 * 1024.0 * 1024.0
    
    // Aim to increase by the remaining capacity of the link
    var incVal = nicSpeed - this.tempBps
    if (incVal < 0.0) {
      incVal = 0.0
    }
    
    // Calculate the expected time till the next update
    val secElapsed = (System.currentTimeMillis - lastUpdateTime) / 1000.0
    val timeTillUpdate = 1.0 * VarysCommon.HEARTBEAT_SEC - secElapsed

    // Bound incVal by blockSize
    if (timeTillUpdate > 0.0) {
      val temp = blockSize / timeTillUpdate
      if (temp < incVal) {
        incVal = temp
      }
    }
    
    this.tempBps += incVal
  }
}
