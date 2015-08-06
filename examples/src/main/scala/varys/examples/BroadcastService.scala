package varys.examples

import java.io._
import java.net._
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.concurrent.{Future, Await, ExecutionContext}

import varys.util.AkkaUtils
import varys.{Logging, Utils}
import varys.framework.client._
import varys.framework._

/**
 * HOWTO Run the BroadcastService Example
 * ======================================
 *
 * BroadcastService consists of two pieces: the sender/master and the receivers/clients.
 * Each one has its own process and must be started separately. 
 *
 * First, we must start the BroadcastSender by providing varysMasterUrl, pathToFile, and 
 * numSlaves. Note the broadcast Master Url printed on console.
 * 
 * Next, we start the BroadcastReceiver by providing varysMasterUrl and broadcast Master Url.
 */

private[varys] object BroadcastUtils {
  
  val BLOCK_SIZE = 1 * 1048576
  val BROADCAST_MASTER_PORT = 1608
  
  val BROADCAST_SLAVE_NUM_RETRIES = 5
  val BROADCAST_SLAVE_RETRY_INTERVAL_MS = 1000
  
}

private[varys] case class BroadcastInfo(
    val coflowId: Int, 
    val pathToFile: String, 
    val LEN_BYTES: Long)
private[varys] case class BroadcastRequest()
private[varys] case class BroadcastDone()
private[varys] case class PieceRequest(
    val offset: Int, 
    val sizeInBytes: Int)

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
    
    val HEARTBEAT_SEC = System.getProperty("varys.framework.heartbeat", "1").toInt
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
            serverSocket.setSoTimeout(HEARTBEAT_SEC * 1000)
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
                  val oos = new ObjectOutputStream(
                    new BufferedOutputStream(new VarysOutputStream(clientSocket, bInfo.coflowId)))
                  oos.flush
                  val ois = new ObjectInputStream(clientSocket.getInputStream)

                  try {
                    val msg = ois.readObject
                    msg match {
                      case br: BroadcastRequest => {
                        // Mark start of slave connection
                        val bMsg1 = msg.asInstanceOf[BroadcastRequest]
                        connectedSlaves.getAndIncrement()
                        
                        // Send file information
                        oos.writeObject(bInfo)
                        oos.flush
                        
                        // Mark end of slave connection
                        val bMsg2 = ois.readObject.asInstanceOf[BroadcastDone]
                        finishedSlaves.getAndIncrement()
                      }
                      case pr: PieceRequest => {
                        // Parse
                        val pMsg = msg.asInstanceOf[PieceRequest]
                        
                        // Read from file
                        val randFile = new RandomAccessFile(bInfo.pathToFile, "r")
                        randFile.seek(pMsg.offset)
                        val bArr = new Array[Byte](pMsg.sizeInBytes)
                        randFile.read(bArr, 0, pMsg.sizeInBytes)
                        randFile.close()

                        // Send
                        oos.writeObject(bArr)
                        oos.flush
                      }
                      case _ => {
                        throw new Exception("Unknown request to BroadcastMaster")
                      }
                    }
                  } catch {
                    case e: Exception => {
                      logWarning (serverThreadName + " had a " + e)
                    }
                  } finally {
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
      case e: Exception => logError(e.toString)
      exitGracefully(1)
    }
    val LEN_BYTES = FILE.length
    
    var numBlocks = (LEN_BYTES / BroadcastUtils.BLOCK_SIZE).toInt
    if (LEN_BYTES % BroadcastUtils.BLOCK_SIZE > 0) {
      numBlocks += 1
    }

    val listener = new TestListener
    val client = new VarysClient("BroadcastSender", url, listener)
    client.start()
    
    val desc = new CoflowDescription(
      "Broadcast-" + fileName, 
      CoflowType.BROADCAST, 
      numBlocks * numSlaves, 
      LEN_BYTES * numSlaves)

    val coflowId = client.registerCoflow(desc)
    logInfo("Registered coflow " + coflowId)
    
    // Start server after registering the coflow and relevant 
    val masterThread = new MasterThread(BroadcastInfo(coflowId, pathToFile, LEN_BYTES), numSlaves, 
      coflowId.toString)
    masterThread.start()
    logInfo("Started MasterThread. Now waiting for it to die.")
    logInfo("Broadcast Master Url: %s:%d".format(
        Utils.localHostName, BroadcastUtils.BROADCAST_MASTER_PORT))
    logInfo("Number of blocks: %d".format(numBlocks))

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

  // ExecutionContext for Futures
  implicit val futureExecContext = ExecutionContext.fromExecutor(Utils.newDaemonCachedThreadPool())

  private def createSocket(host: String, port: Int): Socket = {
    var retriesLeft = BroadcastUtils.BROADCAST_SLAVE_NUM_RETRIES
    while (retriesLeft > 0) {
      try {
        val sock = new Socket(host, port)
        return sock
      } catch {
        case e: Exception => { 
          logWarning("Failed to connect to " + host + ":" + port + " due to " + e.toString)
        }
      }
      Thread.sleep(BroadcastUtils.BROADCAST_SLAVE_RETRY_INTERVAL_MS)
      retriesLeft -= 1
    }
    null
  }

  def exitGracefully(exitCode: Int) {
    if (sock != null)
      sock.close
    if (FILE != null)
      FILE.close

    System.exit(exitCode)
  }

  def main(args: Array[String]) {
    if (args.length < 1) {
      println("USAGE: BroadcastReceiver <broadcastMasterUrl>")
      System.exit(1)
    }
    
    val bUrl = args(0)
    
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
    
    oos = new ObjectOutputStream(new BufferedOutputStream(sock.getOutputStream))
    oos.flush
    ois = new ObjectInputStream(sock.getInputStream)
    
    // Mark start
    oos.writeObject(BroadcastRequest())
    oos.flush

    // Receive FileInfo
    bInfo = ois.readObject.asInstanceOf[BroadcastInfo]
    logInfo("Preparing to receive " + bInfo)
    
    // Open file and setup variables
    var origPathToFile = bInfo.pathToFile
    var origFileName: String = null
    var localPathToFile = bInfo.pathToFile
    var localFileName: String = null
    try {
      var tmpFile = new File(origPathToFile)
      localPathToFile = origPathToFile
      origFileName = tmpFile.getName()
      localFileName = origFileName

      // If pathToFile exists, rename both localPathToFile and localFileName. 
      // Required ONLY for local mode to avoid overwriting the original file.
      if (tmpFile.exists) {
        localPathToFile += ".NEW"
        tmpFile = new File(localPathToFile)
        localFileName += ".NEW"
      }

      // Create parent directory, if required
      if (!tmpFile.getParentFile.exists) {
        tmpFile.getParentFile.mkdirs
      }
      
      // Now create the RandomAccessFile
      FILE = new RandomAccessFile(tmpFile, "rw")
    } catch {
      case e: Exception => logError(e.toString)
      exitGracefully(1)
    }
    
    // Create a random order of blocks (Array[(offset, blockSize)])
    val allOffsets = new ArrayBuffer[(Int, Int)]
    for (fromBytes <- 0L until bInfo.LEN_BYTES by BroadcastUtils.BLOCK_SIZE) {
      val blockSize = if (fromBytes + BroadcastUtils.BLOCK_SIZE >= bInfo.LEN_BYTES) {
        bInfo.LEN_BYTES - fromBytes
      } else {
        BroadcastUtils.BLOCK_SIZE
      }
      allOffsets += ((fromBytes.toInt, blockSize.toInt))
    }
    val randomOffsets = Utils.randomize(allOffsets)
    
    logInfo("About to receive " + bInfo + " with " + randomOffsets.size + " blocks.")
    val futureList = Future.traverse(randomOffsets)(pInfo => Future {

      val blockName = origFileName + "-" + pInfo._1
      logInfo("Getting " + blockName + " from coflow " + bInfo.coflowId)
      
      // Connect to broadcast master, retry silently if required
      val pieceSock = createSocket(masterHost, masterPort)
      if (pieceSock == null) {
        exitGracefully(1)
      }
      
      val poos = new ObjectOutputStream(new BufferedOutputStream(pieceSock.getOutputStream))
      poos.flush
      val pois = new ObjectInputStream(pieceSock.getInputStream)
      
      // Mark start
      poos.writeObject(PieceRequest(pInfo._1, pInfo._2))
      poos.flush

      val bArr = pois.readObject.asInstanceOf[Array[Byte]]

      logInfo("Got " + blockName + " of " + bArr.length + " bytes. Writing to " + localPathToFile + 
        " at " + pInfo._1)

      FILE.synchronized {
        FILE.seek(pInfo._1)
        FILE.write(bArr)
      }

      // Close socket and streams
      pois.close
      pieceSock.close
    })

    Await.result(futureList, Duration.Inf)

    // Mark end
    oos.writeObject(BroadcastDone())
    oos.flush
    
    // Close everything
    exitGracefully(0)
  }
}
