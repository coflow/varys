package varys.examples

import varys.util.AkkaUtils
import varys.{Logging, Utils}
import varys.framework.client._
import varys.framework._

private[varys] object ReceiverClientObject {

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
    if (args.length < 2) {
      println("USAGE: ReceiverClientObject <masterUrl> <coflowId> [objectName]")
      System.exit(1)
    }
    
    val url = args(0)
    val coflowId = args(1)
    val OBJ_NAME = if (args.length > 2) args(2) else "OBJ"

    val listener = new TestListener
    val client = new VarysClient("ReceiverClientObject", url, listener)
    client.start()
    
    Thread.sleep(5000)
    
    println("Trying to retrieve " + OBJ_NAME)
    val respArr = client.getObject[Array[Int]](OBJ_NAME, coflowId)
    println("Got " + OBJ_NAME + " with " + respArr.length + " elements. Now waiting to die.")
    
    client.awaitTermination()
  }
}
