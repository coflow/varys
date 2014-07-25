package varys.examples

import varys.util.AkkaUtils
import varys.{Logging, Utils}
import varys.framework.client._
import varys.framework._

private[varys] object SenderClientFake {

  val LEN_BYTES = 1010101L
  var DATA_NAME = "DATA"

  class TestListener extends ClientListener with Logging {
    def connected(id: String) {
      logInfo("Connected to master, got client ID " + id)
    }

    def disconnected() {
      logInfo("Disconnected from master")
      System.exit(0)
    }
  }

  /*
   * This passes sender information to the receiver, which actually should've happened through the
   * framework master.
   */ 
  def getDataDescription(client: VarysClient, coflowId: String): FlowDescription = {
    client.createFakeDescription(DATA_NAME, coflowId, LEN_BYTES, 1)
  }

  def main(args: Array[String]) {
    if (args.length < 1) {
      println("USAGE: SenderClientFake <masterUrl> [dataName]")
      System.exit(1)
    }

    val url = args(0)
    DATA_NAME = if (args.length > 1) args(1) else "DATA"

    val listener = new TestListener
    val client = new VarysClient("SenderClientFake", url, listener)
    client.start()

    val desc = new CoflowDescription("DEFAULT", CoflowType.DEFAULT, 1, LEN_BYTES)
    val coflowId = client.registerCoflow(desc)
    
    val SLEEP_MS1 = 5000    
    println("Registered coflow " + coflowId + ". Now sleeping for " + SLEEP_MS1 + " milliseconds.")
    Thread.sleep(SLEEP_MS1)

    // Do nothing really. 
    println("Put a fake piece of data of " + LEN_BYTES + " bytes. Now waiting to die.")
    
    // client.unregisterCoflow(coflowId)
    
    client.awaitTermination()
  }
}
