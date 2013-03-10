package varys.framework

import java.io.{File, ObjectInputStream, ObjectOutputStream}
import java.net.{Socket, ServerSocket}

import scala.collection.mutable.HashMap

import varys.{VarysCommon, Logging, Utils}

import com.google.common.io.Files

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
  
  var serverSocket: ServerSocket = new ServerSocket(commPort)
  
  var stopServer = false
  val serverThread = new Thread(serverThreadName) {
    override def run() {
      var threadPool = Utils.newDaemonCachedThreadPool

      try {
        while (!stopServer) {
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
                    val req = ois.readObject.asInstanceOf[GetRequest]
                    val toSend: Option[Array[Byte]] = req.flowDesc.dataType match {

                      case DataType.FAKE => {
                        // Create Data
                        val size = req.flowDesc.sizeInBytes.toInt
                        Some(Array.tabulate[Byte](size)(_.toByte))
                      }

                      case DataType.ONDISK => {
                        // Read data from file into memory and send it
                        val fileDesc = req.flowDesc.asInstanceOf[FileDescription]
                        Some(Files.toByteArray(new File(fileDesc.pathToFile)))
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