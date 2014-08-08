package varys.examples

import java.io._
import java.net._

import varys.Logging
import varys.framework.client._
import varys.framework._

private[examples] object VarysExampleServer {
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
      println("USAGE: VarysExampleServer <varysMasterUrl> <serverPort>")
      System.exit(1)
    }

    val url = args(0)
    val serverPort = args(1).toInt
        
    // First, register coflow.
    val client = new VarysClient("VarysExampleServer", url, new TestListener)
    client.start()

    val desc = new CoflowDescription("DEFAULT", CoflowType.DEFAULT, -1, -1)
    val coflowId = client.registerCoflow(desc)
    
    println("Registered coflow " + coflowId + ". Now waiting for clients.")

    val serverSocket = new ServerSocket(serverPort)
    val clientSocket = serverSocket.accept
    
    System.out.println("Serving client " + clientSocket)
    val ois = new ObjectInputStream(clientSocket.getInputStream)
    val out = new VarysOutputStream(clientSocket, coflowId)
    // val out = new ObjectOutputStream(clientSocket.getOutputStream)
    
    try {
      val reqSizeMB = ois.readLong
      val totBytes = reqSizeMB * 1048576       
      val buf = new Array[Byte](65535)
      var bytesSent = 0L
      while (bytesSent < totBytes) {
        val bytesToSend = 
          math.min(totBytes - bytesSent, buf.length)

        out.write(buf, 0, bytesToSend.toInt)
        bytesSent += bytesToSend
        System.out.println("Sent " + bytesSent + " bytes of " + totBytes)
      }
    } catch {
      case e: Exception => {
        System.out.println("Server had a " + e)
      }
    } finally {
      out.close
      clientSocket.close
    }
    serverSocket.close

    // Finally, unregister coflow
    client.unregisterCoflow(coflowId)
  }  
}
