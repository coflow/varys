package varys.framework.client

import varys.util.AkkaUtils
import varys.{Logging, Utils}
import varys.framework._

private[varys] object ReceiverClient {

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
    val coflowId = args(1)

    val listener = new TestListener
    val client = new Client("ReceiverClient", url, listener)
    client.start()

    client.awaitTermination()
  }
}
