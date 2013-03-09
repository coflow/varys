package varys.framework.client

import java.io._
import java.net._

import scala.collection.mutable.HashMap

import akka.actor._
import akka.pattern.ask
import akka.util.duration._
import akka.pattern.AskTimeoutException
import akka.remote.RemoteClientLifeCycleEvent
import akka.remote.RemoteClientShutdown
import akka.remote.RemoteClientDisconnected
import akka.actor.Terminated
import akka.dispatch.Await

import varys.{VarysCommon, VarysException, Logging}
import varys.framework._
import varys.framework.master.{Master, CoflowInfo}
import varys.framework.slave.Slave
import varys.util._
import varys.Utils

private[varys] class Client(
    clientName: String,
    masterUrl: String,
    listener: ClientListener)
  extends Logging {

  val INTERNAL_ASK_TIMEOUT_MS: Int = System.getProperty("varys.framework.ask.wait", "5000").toInt

  var actorSystem: ActorSystem = null
  
  var masterActor: ActorRef = null
  val clientRegisterLock = new Object
  
  var slaveId: String = null
  var slaveUrl: String = null
  var slaveActor: ActorRef = null
  
  var clientId: String = null
  var clientActor: ActorRef = null

  val flowToThrottledInputStream = new HashMap[DataIdentifier, ThrottledInputStream]
  val flowToObject = new HashMap[DataIdentifier, Array[Byte]]

  var serverSocket = new ServerSocket(0)
  var clientHost = Utils.localHostName()
  var clientCommPort = serverSocket.getLocalPort

  var stopServer = false
  val serverThreadName = "ServerThread for Slave@" + Utils.localHostName()
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
                      case DataType.INMEMORY => {
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
                      logInfo (serverThreadName + " had a " + e)
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
              case ioe: IOException => {
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
  serverThread.start()

  class ClientActor extends Actor with Logging {
    var masterAddress: Address = null
    var alreadyDisconnected = false  // To avoid calling listener.disconnected() multiple times

    override def preStart() {
      logInfo("Connecting to master " + masterUrl)
      try {
        masterActor = context.actorFor(Master.toAkkaUrl(masterUrl))
        masterAddress = masterActor.path.address
        masterActor ! RegisterClient(clientName, clientHost, clientCommPort)
        context.system.eventStream.subscribe(self, classOf[RemoteClientLifeCycleEvent])
        context.watch(masterActor)  // Doesn't work with remote actors, but useful for testing
      } catch {
        case e: Exception =>
          logError("Failed to connect to master", e)
          markDisconnected()
          context.stop(self)
      }
    }

    override def receive = {
      case RegisteredClient(clientId_, slaveId_, slaveUrl_) =>
        clientId = clientId_
        slaveId = slaveId_
        slaveUrl = slaveUrl_
        slaveActor = context.actorFor(Slave.toAkkaUrl(slaveUrl))
        listener.connected(clientId)
        clientRegisterLock.synchronized { clientRegisterLock.notifyAll() }  // Ready to go!
        logInfo("Registered to master. Local slave url = " + slaveUrl)

      case Terminated(actor_) if actor_ == masterActor =>
        logError("Connection to master failed; stopping client")
        markDisconnected()
        context.stop(self)

      case RemoteClientDisconnected(_, address) if address == masterAddress =>
        logError("Connection to master failed; stopping client")
        markDisconnected()
        context.stop(self)

      case RemoteClientShutdown(_, address) if address == masterAddress =>
        logError("Connection to master failed; stopping client")
        markDisconnected()
        context.stop(self)

      case StopClient =>
        markDisconnected()
        sender ! true
        context.stop(self)
        
      case UpdatedShares(newShares) => 
        for ((flowDesc, newBPS) <- newShares) {
          if (flowToThrottledInputStream.contains(flowDesc.dataId)) {
            flowToThrottledInputStream(flowDesc.dataId).updateRate(newBPS)
          }
        }
    }

    /**
     * Notify the listener that we disconnected, if we hadn't already done so before.
     */
    def markDisconnected() {
      if (!alreadyDisconnected) {
        listener.disconnected()
        alreadyDisconnected = true
      }
    }
    
  }

  def start() {
    // Just launch an actor; it will call back into the listener.
    val (actorSystem_, _) = AkkaUtils.createActorSystem("varysClient", Utils.localIpAddress, 0)
    actorSystem = actorSystem_
    clientActor = actorSystem.actorOf(Props(new ClientActor))
  }

  def stop() {
    if (clientActor != null) {
      try {
        val timeout = INTERNAL_ASK_TIMEOUT_MS.millis
        val future = clientActor.ask(StopClient)(timeout)
        Await.result(future, timeout)
      } catch {
        case e: AskTimeoutException =>  // Ignore it, maybe master went away
      }
      clientActor = null
    }
    stopServer = true
  }
  
  def awaitTermination() { 
    actorSystem.awaitTermination() 
  }
  
  def registerCoflow(coflowDesc: CoflowDescription): String = {
    // Wait until the client has been registered
    while (clientId == null) {
      clientRegisterLock.synchronized { 
        clientRegisterLock.wait() 
      }
    }
    
    // Register with the master
    val RegisteredCoflow(coflowId) = AkkaUtils.askActorWithReply[RegisteredCoflow](masterActor, 
      RegisterCoflow(clientId, coflowDesc))
      
    // Let the local slave know
    AkkaUtils.tellActor(slaveActor, RegisteredCoflow(coflowId))
    
    coflowId
  }
  
  def unregisterCoflow(coflowId: String) {
    while (clientId == null) {
      clientRegisterLock.synchronized { 
        clientRegisterLock.wait() 
      }
    }
    
    // Let the master know
    AkkaUtils.tellActor(masterActor, UnregisterCoflow(coflowId))
    
    // Update local slave
    AkkaUtils.tellActor(slaveActor, UnregisterCoflow(coflowId))
    
    // Free local resources
    flowToThrottledInputStream.retain((dataId, _) => dataId.coflowId != coflowId)
    flowToObject.retain((dataId, _) => dataId.coflowId != coflowId)
  }

  /**
   * Makes data available for retrieval, and notifies local slave, which will register it with the master.
   * Non-blocking call.
   */
  private def handlePut(flowDesc: FlowDescription, serialObj: Array[Byte] = null) {
    // Notify the slave, which will notify the master
    AkkaUtils.tellActor(slaveActor, AddFlow(flowDesc))
    
    // Keep a reference to the object to be served when asked for.
    if (flowDesc.dataType == DataType.INMEMORY) {
      assert(serialObj != null)
      flowToObject(flowDesc.dataId) = serialObj
    } 
  }

  /**
   * Puts any data structure
   */
  def putObject[T: Manifest](objId: String, obj: T, coflowId: String, size: Long, numReceivers: Int) {
    // TODO: Figure out class name
    val className = "UnknownType" 
    val desc = new ObjectDescription(objId, className, coflowId, DataType.INMEMORY, size, 
      numReceivers, clientHost, clientCommPort)
    val serialObj = Utils.serialize[T](obj)
    handlePut(desc, serialObj)
  }
  
  /**
   * Puts a local file
   */
  def putFile(fileId: String, pathToFile: String, coflowId: String, size: Long, numReceivers: Int) {
    val desc = new FileDescription(fileId, pathToFile, coflowId, DataType.ONDISK, size, numReceivers, 
      clientHost, clientCommPort)
    handlePut(desc)
  }
  
  /**
   * Emulates the process without having to actually put anything
   */
  def putFake(blockId: String, coflowId: String, size: Long, numReceivers: Int) {
    val desc = new FlowDescription(blockId, coflowId, DataType.FAKE, size, numReceivers, 
      clientHost, clientCommPort)
    handlePut(desc)
  }
  
  /**
   * Notifies the master and the slave. But everything is done in the client
   * Blocking call.
   */
  private def handleGet(blockId: String, dataType: DataType.DataType, coflowId: String): Array[Byte] = {
    // Notify master and retrieve the FlowDescription in response
    val GotFlowDesc(flowDesc) = AkkaUtils.askActorWithReply[GotFlowDesc](masterActor, 
      GetFlow(blockId, coflowId, clientId, slaveId))
    
    // Notify local slave
    AkkaUtils.tellActor(slaveActor, GetFlow(blockId, coflowId, clientId, slaveId, flowDesc))
    
    // Get it!
    val sock = new Socket(flowDesc.originHost, flowDesc.originCommPort)
    val oos = new ObjectOutputStream(sock.getOutputStream)
    oos.flush
    val tis = new ThrottledInputStream(sock.getInputStream)
    val ois = new ObjectInputStream(tis)
    
    flowToThrottledInputStream(flowDesc.dataId) = tis
    
    oos.writeObject(GetRequest(flowDesc))
    oos.flush
    
    val resp = ois.readObject.asInstanceOf[Option[Array[Byte]]]
    var retVal: Array[Byte] = null
    resp match {
      case Some(byteArr) => {
        logInfo("Received response of " + byteArr.length + " bytes")
        
        dataType match {
          case DataType.FAKE => {
            // Throw away
          }

          case DataType.ONDISK => {
            // TODO: Write to disk or something else
            retVal = byteArr
          }

          case DataType.INMEMORY => {
            // TODO: Do something
            retVal = byteArr
          }

          case _ => {
            logError("Invalid DataType!")
            throw new VarysException("Invalid DataType!")
          }
        }
      }
      case None => {
        logError("Nothing received!")
        throw new VarysException("Invalid DataType!")
      }
    }
    
    ois.close
    oos.close
    sock.close
    
    return retVal
  }
  
  /**
   * Retrieves data from any of the feasible locations. 
   */
  def getObject[T](objectId: String, coflowId: String): T = {
    val resp = handleGet(objectId, DataType.INMEMORY, coflowId)
    Utils.deserialize[T](resp)
  }
  
  /**
   * Gets a file
   */
  def getFile(fileId: String, coflowId: String) {
    handleGet(fileId, DataType.ONDISK, coflowId)
  }
  
  /**
   * Paired get() for putFake. Doesn't return anything, but emulates the retrieval process.
   */
  def getFake(blockId: String, coflowId: String) {
    handleGet(blockId, DataType.FAKE, coflowId)
  }
  
  def delete(flowId: String, coflowId: String) {
    AkkaUtils.tellActor(slaveActor, DeleteFlow(flowId, coflowId))
  }

}
