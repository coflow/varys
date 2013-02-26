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
    val url = args(0)
    val listener = new TestListener
    val client = new Client("SenderClient", url, listener)
    client.start()

    val desc = new CoflowDescription("DEFAULT", CoflowType.DEFAULT, 100)
    val coflowId = client.registerCoflow(desc)
    
    client.awaitTermination()
  }
}
