package varys.examples

import java.io._
import java.net._
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer
  
import varys.util.AkkaUtils
import varys.{Logging, Utils, VarysCommon}
import varys.framework.client._
import varys.framework._

private[varys] object BroadcastUtils {
  
  val BLOCK_SIZE = 1 * 1048576
  val BROADCAST_MASTER_PORT = 1608
  
  val BROADCAST_SLAVE_NUM_RETRIES = 5
  val BROADCAST_SLAVE_RETRY_INTERVAL_MS = 1000
  
}

private[varys] case class BroadcastInfo(
    val coflowId: String, 
    val pathToFile: String, 
    val LEN_BYTES: Long)
private[varys] case class BroadcastRequest()
private[varys] case class BroadcastDone()

private[varys] object BroadcastSender extends Logging {

  class TestListener extends ClientListener with Logging {
    def connected(id: String) {
      logInfo("Connected to master, got client ID " + id)
    }

    def disconnected() {
      logInfo("Disconnected from master")
      System.exit(0)
    }
  }
  
  class MasterThread (
      val bInfo: BroadcastInfo,
      val numSlaves: Int,
      val serverThreadName: String = "BroadcastMaster") 
    extends Thread (serverThreadName) with Logging {
    
    var serverSocket: ServerSocket = new ServerSocket(BroadcastUtils.BROADCAST_MASTER_PORT)
    
    var connectedSlaves = new AtomicInteger()
    var finishedSlaves = new AtomicInteger()
    
    var stopServer = false
    this.setDaemon(true)
    
    override def run() {
      var threadPool = Utils.newDaemonCachedThreadPool

      try {
        while (!stopServer && !finished) {
          var clientSocket: Socket = null
          try {
            serverSocket.setSoTimeout(VarysCommon.HEARTBEAT_SEC * 1000)
            clientSocket = serverSocket.accept
          } catch {
            case e: Exception => { 
              if (stopServer) {
                logInfo("Stopping " + serverThreadName)
              }
            }
          }

          if (clientSocket != null) {
            try {
              threadPool.execute (new Thread {
                override def run: Unit = {
                  val oos = new ObjectOutputStream(clientSocket.getOutputStream)
                  oos.flush
                  val ois = new ObjectInputStream(clientSocket.getInputStream)

                  try {
                    // Mark start of slave connection
                    val bMsg1 = ois.readObject.asInstanceOf[BroadcastRequest]
                    connectedSlaves.getAndIncrement()
                    
                    // Send file information
                    oos.writeObject(bInfo)
                    oos.flush
                    
                    // Mark end of slave connection
                    val bMsg2 = ois.readObject.asInstanceOf[BroadcastDone]
                    finishedSlaves.getAndIncrement()
                  } catch {
                    case e: Exception => {
                      logWarning (serverThreadName + " had a " + e)
                    }
                  } finally {
                    ois.close
                    oos.close
                    clientSocket.close
                  }
                }
              })
            } catch {
              // In failure, close socket here; else, client thread will close
              case e: Exception => {
                logError (serverThreadName + " had a " + e)
                clientSocket.close
              }
            }
          }
        }
      } finally {
        serverSocket.close
      }
      // Shutdown the thread pool
      threadPool.shutdown
    }
    
    def stopMaster() {
      stopServer = true
    }
    
    def finished = (finishedSlaves.get() == numSlaves)
  }

  var FILE: RandomAccessFile = null

  def exitGracefully(exitCode: Int) {
    if (FILE != null)
      FILE.close

    System.exit(exitCode)
  }
  
  def main(args: Array[String]) {
    if (args.length < 3) {
      println("USAGE: BroadcastSender <varysMasterUrl> <pathToFile> <numSlaves>")
      System.exit(1)
    }

    val url = args(0)
    val pathToFile = args(1)
    val numSlaves = args(2).toInt

    var fileName: String = null
    try {
      val tmpFile = new File(pathToFile)
      fileName = tmpFile.getName()
      FILE = new RandomAccessFile(tmpFile, "r")
    } catch {
      case e => logError(e.toString)
      exitGracefully(1)
    }
    val LEN_BYTES = FILE.length
    
    val listener = new TestListener
    val client = new Client("BroadcastSender", url, listener)
    client.start()
    
    val desc = new CoflowDescription("Broadcast-" + fileName, CoflowType.BROADCAST, numSlaves)
    val coflowId = client.registerCoflow(desc)
    logInfo("Registered coflow " + coflowId)
    
    // PUT blocks of the input file
    for (fromBytes <- 0L to LEN_BYTES by BroadcastUtils.BLOCK_SIZE) {
      val blockSize = if (fromBytes + BroadcastUtils.BLOCK_SIZE >= LEN_BYTES) {
        LEN_BYTES - fromBytes
      } else {
        BroadcastUtils.BLOCK_SIZE
      }
      
      val blockName = fileName + "-" + fromBytes
      logInfo("Putting " + blockName + " into coflow " + coflowId)
      client.putFile(blockName, pathToFile, coflowId, fromBytes, blockSize, numSlaves)
    }

    // Start server after registering the coflow and relevant 
    val masterThread = new MasterThread(BroadcastInfo(coflowId, pathToFile, LEN_BYTES), numSlaves)
    masterThread.start()
    logInfo("Started MasterThread. Now waiting for it to die.")

    // Wait for all slaves to receive
    masterThread.join()
    FILE.close
    
    logInfo("Unregistered coflow " + coflowId)
    client.unregisterCoflow(coflowId)
  }
}

private[varys] object BroadcastReceiver extends Logging {
  private val broadcastMasterUrlRegex = "([^:]+):([0-9]+)".r
  
  var sock: Socket = null
  var oos: ObjectOutputStream = null
  var ois: ObjectInputStream = null
  var FILE: RandomAccessFile = null

  class TestListener extends ClientListener with Logging {
    def connected(id: String) {
      logInfo("Connected to master, got client ID " + id)
    }

    def disconnected() {
      logInfo("Disconnected from master")
      System.exit(0)
    }
  }

  private def createSocket(host: String, port: Int): Socket = {
    var retriesLeft = BroadcastUtils.BROADCAST_SLAVE_NUM_RETRIES
    while (retriesLeft > 0) {
      try {
        val sock = new Socket(host, port)
        return sock
      } catch {
        case e => { 
          logWarning("Failed to connect to " + host + ":" + port + " due to " + e.toString)
        }
      }
      Thread.sleep(BroadcastUtils.BROADCAST_SLAVE_RETRY_INTERVAL_MS)
      retriesLeft -= 1
    }
    null
  }

  def exitGracefully(exitCode: Int) {
    if (ois != null)
      ois.close
    if (oos != null) 
      oos.close
    if (sock != null)
      sock.close
    if (FILE != null)
      FILE.close

    System.exit(exitCode)
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      println("USAGE: BroadcastReceiver <varysMasterUrl> <broadcastMasterUrl>")
      System.exit(1)
    }
    
    val url = args(0)
    val bUrl = args(1)
    
    var masterHost: String = null
    var masterPort: Int = 0
    var bInfo: BroadcastInfo = null
    
    bUrl match {
      case broadcastMasterUrlRegex(h, p) => 
        masterHost = h
        masterPort = p.toInt
      case _ =>
        logError("Invalid broadcastMasterUrl: " + bUrl)
        logInfo("broadcastMasterUrl should be given as host:port")
        exitGracefully(1)
    }

    // Connect to broadcast master, retry silently if required
    sock = createSocket(masterHost, masterPort)
    if (sock == null) {
      exitGracefully(1)
    }
    
    oos = new ObjectOutputStream(sock.getOutputStream)
    oos.flush
    ois = new ObjectInputStream(sock.getInputStream)
    
    // Mark start
    oos.writeObject(BroadcastRequest())
    oos.flush

    // Receive FileInfo
    bInfo = ois.readObject.asInstanceOf[BroadcastInfo]
    
    // Open file and setup variables
    var pathToFile = bInfo.pathToFile
    var fileName: String = null    
    try {
      var tmpFile = new File(pathToFile)
      fileName = tmpFile.getName()

      // If pathToFile exists, rename both pathToFile and fileName. 
      // Required ONLY for local mode to avoid overwriting the original file.
      if (tmpFile.exists) {
        pathToFile += ".NEW"
        tmpFile = new File(pathToFile)
        fileName += ".NEW"
      }

      // Create parent directory, if required
      if (!tmpFile.getParentFile.exists) {
        tmpFile.getParentFile.mkdirs
      }
      
      // Now create the RandomAccessFile
      FILE = new RandomAccessFile(tmpFile, "rw")
    } catch {
      case e => logError(e.toString)
      exitGracefully(1)
    }
    val LEN_BYTES = bInfo.LEN_BYTES
    
    // Create a random order of blocks
    val allOffsets = new ArrayBuffer[Int]
    for (fromBytes <- 0L to LEN_BYTES by BroadcastUtils.BLOCK_SIZE) {
      val blockSize = if (fromBytes + BroadcastUtils.BLOCK_SIZE >= LEN_BYTES) {
        LEN_BYTES - fromBytes
      } else {
        BroadcastUtils.BLOCK_SIZE
      }
      allOffsets += fromBytes.toInt
    }
    val randomOffsets = Utils.randomize(allOffsets)
    
    // Now create coflow client
    val listener = new TestListener
    val client = new Client("BroadcastReceiver", url, listener)
    client.start()
    
    // Receive blocks in random order
    randomOffsets.foreach { offset =>
      val blockName = fileName + "-" + offset
      logInfo("Getting " + blockName + " from coflow " + bInfo.coflowId)
      val bArr = client.getFile(blockName, bInfo.coflowId)
      logInfo("Got " + blockName + " of " + bArr.length + " bytes. Writing to " + pathToFile + " at " + offset)
      FILE.seek(offset)
      FILE.write(bArr)
    }
    
    // Mark end
    oos.writeObject(BroadcastDone())
    oos.flush
    
    // Close everything
    exitGracefully(0)
    
  }
}
