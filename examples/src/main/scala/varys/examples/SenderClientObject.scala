package varys.examples

import varys.util.AkkaUtils
import varys.{Logging, Utils}
import varys.framework.client._
import varys.framework._

private[varys] object SenderClientObject {

  class TestListener extends ClientListener with Logging {
    def connected(id: String) {
      logInfo("Connected to master, got client ID " + id)
    }

    def disconnected() {
      logInfo("Disconnected from master")
      System.exit(0)
    }
  }

  def main(args: Array[String]) {
    if (args.length < 1) {
      println("USAGE: SenderClientObject <masterUrl> [objectName]")
      System.exit(1)
    }

    val url = args(0)
    val OBJ_NAME = if (args.length > 1) args(1) else "OBJ"

    val NUM_ELEMS = 1231231
    val toSend = Array.tabulate[Int](NUM_ELEMS)(_.toByte)

    val listener = new TestListener
    val client = new VarysClient("SenderClientObject", url, listener)
    client.start()

    val desc = new CoflowDescription("DEFAULT", CoflowType.DEFAULT, 1, NUM_ELEMS * 4)
    val coflowId = client.registerCoflow(desc)
    
    val SLEEP_MS1 = 5000    
    println("Registered coflow " + coflowId + ". Now sleeping for " + SLEEP_MS1 + " milliseconds.")
    Thread.sleep(SLEEP_MS1)
    
    client.putObject[Array[Int]](OBJ_NAME, toSend, coflowId, NUM_ELEMS * 4, 1)
    println("Put an Array[Int] of " + NUM_ELEMS + " elements. Now waiting to die.")
    
    // client.unregisterCoflow(coflowId)
    
    client.awaitTermination()
  }
}
