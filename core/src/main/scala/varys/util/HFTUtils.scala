package varys.util

import org.apache.commons.io.FileUtils

import java.io._

import varys.Logging

/**
 * Various utility classes for working with HFT.
 */
private[varys] object HFTUtils extends Logging {
  val StartAll = 0
  val PauseAll = 1

  val HFT_WORKDIR_PATH = System.getProperty("varys.framework.hftWorkDir", "/tmp/HFT")
  val HFT_LOCAL_SLAVE_PATH = HFT_WORKDIR_PATH + "/SLAVE"

  def createWorkDirPath(id: String): String = {
    HFT_WORKDIR_PATH + "/" + id
  }

  def cleanWorkDir() {
    FileUtils.deleteDirectory(new File(HFT_WORKDIR_PATH))
  }
}
