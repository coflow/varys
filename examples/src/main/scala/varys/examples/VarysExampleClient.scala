package varys.examples

import java.io._
import java.net._

import varys.framework.client.VarysInputStream

private[examples] object VarysExampleClient {
  def readBytes(in: InputStream, bytesToRecv: Long) {
    val buf = new Array[Byte](65535)
    var bytesReceived = 0L
    while (bytesReceived < bytesToRecv) {
      val n = in.read(buf)
      if (n == -1) {
        System.out.println("EOF reached after " + bytesReceived + " bytes")
      } else {
        bytesReceived += n
      }
      System.out.println("Received " + bytesReceived + " bytes of " + bytesToRecv)
    }
  }

  def main(args: Array[String]) {
    if (args.length < 3) {
      println("USAGE: VarysExampleClient <serverHost> <serverPort> <coflowId> [MBToRecv]")
      System.exit(1)
    }

    val server = args(0)
    val serverPort = args(1).toInt
    val coflowId = args(2)

    val MBToRecv: Long = if (args.length > 3) args(3).toInt else 10
    
    val sock = new Socket(server, serverPort)
    val oos = new ObjectOutputStream(new BufferedOutputStream(sock.getOutputStream))
    oos.flush
    
    oos.writeLong(MBToRecv)
    oos.flush
    
    readBytes(new VarysInputStream(sock, coflowId), MBToRecv * 1048576)
    
    sock.close
  }
}
