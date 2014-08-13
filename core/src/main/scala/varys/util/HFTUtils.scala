package varys.util

import org.apache.commons.io.FileUtils

import java.io._

import varys.Logging

/**
 * Various utility classes for working with HFT.
 */
private[varys] object HFTUtils extends Logging {
  val GetReadToken  = 0
  val GetWriteToken = 1
  val ReadToken     = 2
  val WriteToken    = 3

  val HFT_WORKDIR_PATH = "/tmp/HFT"
  val HFT_LOCAL_SLAVE_PATH = HFT_WORKDIR_PATH + "/SLAVE"

  def createWorkDirPath(id: String): String = {
    HFT_WORKDIR_PATH + "/" + id
  }

  def cleanWorkDir() {
    FileUtils.deleteDirectory(new File(HFT_WORKDIR_PATH))
  }
}
