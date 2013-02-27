package varys.framework.client

import varys.util.AkkaUtils
import varys.{Logging, Utils}
import varys.framework._

private[varys] object SenderClient {

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
      println("USAGE: SenderClient <masterUrl> [dataName]")
      System.exit(1)
    }

    val url = args(0)
    val DATA_NAME = if (args.length > 1) args(1) else "DATA"

    val listener = new TestListener
    val client = new Client("SenderClient", url, listener)
    client.start()

    val desc = new CoflowDescription("DEFAULT", CoflowType.DEFAULT, 100)
    val coflowId = client.registerCoflow(desc)
    
    val SLEEP_MS1 = 5000    
    println("Registered coflow " + coflowId + ". Now sleeping for " + SLEEP_MS1 + " milliseconds.")
    Thread.sleep(SLEEP_MS1)
    
    val LEN_BYTES = 1000000L
    client.putFake(DATA_NAME, coflowId, LEN_BYTES)
    println("Put a fake piece of data of " + LEN_BYTES + " bytes. Now waiting to die.")
    
    // client.unregisterCoflow(coflowId)
    
    client.awaitTermination()
  }
}
