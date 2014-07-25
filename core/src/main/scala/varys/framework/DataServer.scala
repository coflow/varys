package varys.framework

import java.io._
import java.net._

import scala.collection.mutable.HashMap

import varys.{Logging, Utils, VarysException}

/**
 * A common server to serve requested pieces of data. 
 * It is used by the Client and Slave classes. 
 * For Clients, commPort == 0, flowObject != null, and DataServer only handles INMEMORY DataType
 * For Slaves, commPort != 0, and DataServer only handles FAKE and ONDISK DataTypes
 */
private[varys] class DataServer(
    val commPort: Int,  // Picks a random port if commPort == 0.
    val serverThreadName: String,
    flowToObject: HashMap[DataIdentifier, Array[Byte]] = null) 
  extends Logging {
  
  val HEARTBEAT_SEC = System.getProperty("varys.framework.heartbeat", "1").toInt
  var serverSocket: ServerSocket = null
  
  try {
    serverSocket = new ServerSocket(commPort, 256)
    logInfo("Created DataServer at %s:%d".format(Utils.localHostName, getCommPort))
    } catch {
      case e: Exception => {
        val errString = "Couldn't create data server due to " + e
        logError(errString)
        throw new VarysException(errString)
      }
    }
  
  var stopServer = false
  val serverThread = new Thread(serverThreadName) {
    override def run() {
      var threadPool = Utils.newDaemonCachedThreadPool

      try {
        while (!stopServer) {
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
                  logInfo("Serving client " + clientSocket)
                  val ois = new ObjectInputStream(clientSocket.getInputStream)
              
                  try {
                    val req = ois.readObject.asInstanceOf[GetRequest]
                    
                    // Specially handle DataType.FAKE
                    if (req.flowDesc.dataType == DataType.FAKE) {
                        val out = clientSocket.getOutputStream
                        val buf = new Array[Byte](65536)
                        var bytesSent = 0L
                        while (bytesSent < req.flowDesc.sizeInBytes) {
                          val bytesToSend = 
                            math.min(req.flowDesc.sizeInBytes - bytesSent, buf.length)
                          
                          out.write(buf, 0, bytesToSend.toInt)
                          bytesSent += bytesToSend
                        }
                    } else {
                      val oos = new ObjectOutputStream(
                        new BufferedOutputStream(clientSocket.getOutputStream))
                      oos.flush
                      
                      val toSend: Option[Array[Byte]] = req.flowDesc.dataType match {

                        case DataType.ONDISK => {
                          // Read the specified amount of data from file into memory and send it
                          val fileDesc = req.flowDesc.asInstanceOf[FileDescription]
                          val randFile = new RandomAccessFile(fileDesc.pathToFile, "r")
                          randFile.seek(fileDesc.offset)
                          val bArr = new Array[Byte](fileDesc.sizeInBytes.toInt)
                          randFile.read(bArr, 0, fileDesc.sizeInBytes.toInt)
                          randFile.close()
                          Some(bArr)
                        }

                        case DataType.INMEMORY => {
                          // Send data if it exists
                          if (flowToObject.contains(req.flowDesc.dataId))
                            Some(flowToObject(req.flowDesc.dataId))
                          else {
                            logWarning("Requested object does not exist!" + flowToObject)
                            None
                          }
                        }

                        case _ => {
                          logWarning("Invalid or Unexpected DataType!")
                          None
                        }
                      }
                      oos.writeObject(toSend)
                      oos.flush
                    }
                  } catch {
                    case e: Exception => {
                      logWarning (serverThreadName + " had a " + e)
                    }
                  } finally {
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
  }
  serverThread.setDaemon(true)
  
  def start() {
    serverThread.start()
  }
  
  def stop() {
    stopServer = true
  }
  
  def getCommPort = serverSocket.getLocalPort
}