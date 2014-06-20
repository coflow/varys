package varys.examples

import java.io._

import varys.util.AkkaUtils
import varys.{Logging, Utils}
import varys.framework.client._
import varys.framework._

private[varys] object SenderClientFile {

  class TestListener extends ClientListener with Logging {
    def connected(id: String) {
      logInfo("Connected to master, got client ID " + id)
    }

    def disconnected() {
      logInfo("Disconnected from master")
      System.exit(0)
    }
  }

  private def write(aInput: Array[Byte], aOutputFileName: String) {
    try {
      var output: OutputStream = null
      try {
        output = new BufferedOutputStream(new FileOutputStream(aOutputFileName))
        output.write(aInput)
      }
      finally {
        output.close()
      }
    } catch {
      case e: Exception => { }
    }
  }
  
  def main(args: Array[String]) {
    if (args.length < 1) {
      println("USAGE: SenderClientFile <masterUrl> [fileName]")
      System.exit(1)
    }

    val url = args(0)
    val FILE_NAME = if (args.length > 1) args(1) else "INFILE"

    val LEN_BYTES = 1212121
    
    val listener = new TestListener
    val client = new VarysClient("SenderClientFile", url, listener)
    client.start()

    val desc = new CoflowDescription("DEFAULT", CoflowType.DEFAULT, 1, LEN_BYTES)
    val coflowId = client.registerCoflow(desc)
    
    val SLEEP_MS1 = 5000    
    println("Registered coflow " + coflowId + ". Now sleeping for " + SLEEP_MS1 + " milliseconds.")
    Thread.sleep(SLEEP_MS1)
    
    val dir = "/tmp"
    val pathToFile = dir + "/" + FILE_NAME

    val byteArr = Array.tabulate[Byte](LEN_BYTES)(_.toByte)
    SenderClientFile.write(byteArr, pathToFile)
    
    client.putFile(FILE_NAME, pathToFile, coflowId, LEN_BYTES, 1)
    println("Put file[" + FILE_NAME + "] of " + LEN_BYTES + " bytes. Now waiting to die.")
    
    // client.unregisterCoflow(coflowId)
    
    client.awaitTermination()
  }
}
