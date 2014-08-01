package varys.framework.client

import akka.actor._
import akka.actor.Terminated
import akka.util.duration._
import akka.pattern.ask
import akka.pattern.AskTimeoutException
import akka.remote.{RemoteClientLifeCycleEvent, RemoteClientDisconnected, RemoteClientShutdown}
import akka.dispatch.{Await, ExecutionContext}

import java.io._
import java.net._

import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._

import varys.{Logging, Utils, VarysException}
import varys.framework._
import varys.framework.master.{Master, CoflowInfo}
import varys.framework.slave.Slave
import varys.util._

class VarysClient(
    clientName: String,
    masterUrl: String,
    listener: ClientListener = null)
  extends Logging {

  val INTERNAL_ASK_TIMEOUT_MS: Int = 
    System.getProperty("varys.client.internalAskTimeoutMillis", "5000").toInt
  val RATE_UPDATE_FREQ = System.getProperty("varys.client.rateUpdateIntervalMillis", "100").toLong
  val NIC_BPS = 1024 * 1048576

  var actorSystem: ActorSystem = null
  
  var masterActor: ActorRef = null
  val masterClientRegisterLock = new Object
  
  var slaveUrl: String = "varys://" + Utils.localHostName + ":1607"
  
  var slaveActor: ActorRef = null
  val slaveClientRegisterLock = new Object
  
  var masterClientId: String = null
  var slaveClientId: String = null

  var clientActor: ActorRef = null

  // ExecutionContext for Futures
  implicit val futureExecContext = ExecutionContext.fromExecutor(Utils.newDaemonCachedThreadPool())

  var masterRegStartTime = 0L
  var slaveRegStartTime = 0L

  val flowToObject = new HashMap[DataIdentifier, Array[Byte]]

  val serverThreadName = "ServerThread for Client@" + Utils.localHostName()
  var dataServer = new DataServer(0, serverThreadName, flowToObject)
  dataServer.start()

  var clientHost = Utils.localHostName()
  var clientCommPort = dataServer.getCommPort

  class ClientActor extends Actor with Logging {
    var masterAddress: Address = null
    var slaveAddress: Address = null

    // To avoid calling listener.disconnected() multiple times
    var alreadyDisconnected = false  

    override def preStart() {
      context.system.eventStream.subscribe(self, classOf[RemoteClientLifeCycleEvent])

      logInfo("Connecting to local slave " + slaveUrl)
      slaveRegStartTime = now
      try {
        slaveActor = context.actorFor(Slave.toAkkaUrl(slaveUrl))
        slaveAddress = slaveActor.path.address
        slaveActor ! RegisterSlaveClient(clientName, clientHost, clientCommPort)
      } catch {
        case e: Exception =>
          logError("Failed to connect to local slave", e)
          markDisconnected()
          context.stop(self)
      }
    }

    @throws(classOf[VarysException])
    def masterDisconnected() {
      // TODO: It would be nice to try to reconnect to the master, but just shut down for now.
      // (Note that if reconnecting we would also need to assign IDs differently.)
      val connToMasterFailedMsg = "Connection to master failed; stopping client"
      logWarning(connToMasterFailedMsg)
      markDisconnected()
      context.stop(self)
      throw new VarysException(connToMasterFailedMsg)
    }

    @throws(classOf[VarysException])
    def slaveDisconnected() {
      // TODO: It would be nice to try to reconnect to the slave, but just shut down for now.
      // (Note that if reconnecting we would also need to assign IDs differently.)
      val connToSlaveFailedMsg = "Connection to local slave failed; stopping client"
      logWarning(connToSlaveFailedMsg)
      markDisconnected()
      context.stop(self)
      throw new VarysException(connToSlaveFailedMsg)
    }

    override def receive = {
      
      case RegisterWithMaster =>
        logInfo("Connecting to master " + masterUrl)
        masterRegStartTime = now
        try {
          masterActor = context.actorFor(Master.toAkkaUrl(masterUrl))
          masterAddress = masterActor.path.address
          masterActor ! RegisterMasterClient(clientName, clientHost, clientCommPort)
          
        } catch {
          case e: Exception =>
            logError("Failed to connect to master", e)
            markDisconnected()
            context.stop(self)
        }

      case RegisteredMasterClient(clientId_, _, _) =>
        masterClientId = clientId_
        if (listener != null) {
          listener.connected(masterClientId)
        }
        masterClientRegisterLock.synchronized { 
          masterClientRegisterLock.notifyAll() 
        }
        logInfo("Registered to master in " +  (now - masterRegStartTime) + " milliseconds.")

      case RegisteredSlaveClient(clientId_) =>
        slaveClientId = clientId_
        slaveClientRegisterLock.synchronized { 
          slaveClientRegisterLock.notifyAll() 
        }
        logInfo("Registered to local slave in " +  (now - slaveRegStartTime) + " milliseconds.")

      case Terminated(actor_) => 
        if (actor_ == masterActor) {
          masterDisconnected()
        } else if (actor_ == slaveActor) {
          slaveDisconnected()
        }

      case RemoteClientDisconnected(_, address) => 
        if (address == masterAddress) {
          masterDisconnected()
        } else if (address == slaveAddress) {
          slaveDisconnected()
        }

      case RemoteClientShutdown(_, address) => 
        if (address == masterAddress) {
          masterDisconnected()
        } else if (address == slaveAddress) {
          slaveDisconnected()
        }

      case StopClient =>
        markDisconnected()
        sender ! true
        context.stop(self)
        
      case UpdatedRates(newRates) => 
        logInfo("Received updated shares, which shouldn't have happened!. Ignoring.")

      case RejectedCoflow(coflowId, rejectMessage) =>
        logDebug("Coflow " + coflowId + " has been rejected! " + rejectMessage)

        // Let the client know
        if (listener != null) {
          listener.coflowRejected(coflowId, rejectMessage)
        }

        // Free local resources
        freeLocalResources(coflowId)
    }

    /**
     * Notify the listener that we disconnected, if we hadn't already done so before.
     */
    def markDisconnected() {
      if (!alreadyDisconnected) {
        if (listener != null) {
          listener.disconnected()
        }
        alreadyDisconnected = true
      }
    }
    
  }

  private def now() = System.currentTimeMillis

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
    dataServer.stop()
  }
  
  def awaitTermination() { 
    actorSystem.awaitTermination() 
  }
  
  // Wait until the client has been registered with the master
  private def waitForMasterRegistration = {
    while (masterClientId == null) {
      clientActor ! RegisterWithMaster
      masterClientRegisterLock.synchronized { 
        masterClientRegisterLock.wait()
        masterClientRegisterLock.notifyAll()
      }
    }
  }
  
  // Wait until the client has been registered with the master
  private def waitForSlaveRegistration = {
    while (slaveClientId == null) {
      slaveClientRegisterLock.synchronized { 
        slaveClientRegisterLock.wait()
        slaveClientRegisterLock.notifyAll()
      }
    }
  }

  def registerCoflow(coflowDesc: CoflowDescription): String = {
    waitForSlaveRegistration
    waitForMasterRegistration

    // Register with the master
    val RegisteredCoflow(coflowId) = AkkaUtils.askActorWithReply[RegisteredCoflow](masterActor, 
      RegisterCoflow(masterClientId, coflowDesc))
      
    // Let the local slave know
    AkkaUtils.tellActor(slaveActor, RegisteredCoflow(coflowId))
    
    coflowId
  }
  
  def unregisterCoflow(coflowId: String) {
    waitForSlaveRegistration
    waitForMasterRegistration
    
    // Let the master know
    AkkaUtils.tellActor(masterActor, UnregisterCoflow(coflowId))
    
    // Update local slave
    AkkaUtils.tellActor(slaveActor, UnregisterCoflow(coflowId))
    
    // Free local resources
    freeLocalResources(coflowId)
  }

  private def freeLocalResources(coflowId: String) {
    flowToObject.retain((dataId, _) => dataId.coflowId != coflowId)
  }
  
  /**
   * Creates FAKE data request
   */
  def createFakeDescription(
      blockId: String, 
      coflowId: String, 
      size: Long, 
      numReceivers: Int): FlowDescription = {
    val desc = 
      new FlowDescription(
        blockId, 
        coflowId, 
        DataType.FAKE, 
        size, 
        numReceivers, 
        clientHost, 
        clientCommPort)
    desc
  }

  /**
   * Creates an entire file request
   */
  def createFileDescription(
      fileId: String, 
      pathToFile: String, 
      coflowId: String, 
      size: Long, 
      numReceivers: Int): FileDescription = {
    val desc = 
      new FileDescription(
        fileId, 
        pathToFile, 
        coflowId, 
        DataType.ONDISK, 
        0, 
        size, 
        numReceivers, 
        clientHost, 
        clientCommPort)
    desc
  }

  /**
   * Creates a file request from given offset
   */
  def createFileDescription(
      fileId: String, 
      pathToFile: String, 
      coflowId: String, 
      offset: Long,
      size: Long, 
      numReceivers: Int): FileDescription = {
    val desc = 
      new FileDescription(
        fileId, 
        pathToFile, 
        coflowId, 
        DataType.ONDISK, 
        offset, 
        size, 
        numReceivers, 
        clientHost, 
        clientCommPort)
    desc
  }

  /**
   * Creates a object request for any data structure
   */
  def createObjectDescription[T: Manifest](
      objId: String, 
      obj: T, 
      coflowId: String, 
      size: Long, 
      numReceivers: Int): ObjectDescription = {
    
    // TODO: Figure out class name
    val className = "UnknownType" 
    val desc = 
      new ObjectDescription(
        objId, 
        className, 
        coflowId, 
        DataType.INMEMORY, 
        size, 
        numReceivers, 
        clientHost, 
        clientCommPort)

    // Keep a reference to the object to be served when asked for.
    flowToObject(DataIdentifier(objId, coflowId)) = Utils.serialize[T](obj)

    desc
  }

  /**
   * Performs exactly one get operation
   */
  @throws(classOf[VarysException])
  private def getOne(flowDesc: FlowDescription): (FlowDescription, Array[Byte]) = {
    waitForSlaveRegistration

    var st = now
    val sock = new Socket(flowDesc.originHost, flowDesc.originCommPort)    
    val oos = new ObjectOutputStream(new BufferedOutputStream(sock.getOutputStream))
    oos.flush

    // Now, notify the local slave
    val sockLocalPort = sock.getLocalPort
    AkkaUtils.tellActor(slaveActor, StartedFlow(flowDesc.coflowId, sockLocalPort))

    val tisRate = NIC_BPS
    val tis = new ThrottledInputStream(sock.getInputStream, clientName, tisRate)
    // logTrace("Created socket and " + tis + " for " + flowDesc + " in " + (now - st) + 
    //   " milliseconds")
    
    oos.writeObject(GetRequest(flowDesc))
    oos.flush
    
    var retVal: Array[Byte] = null
    
    st = now
    // Specially handle DataType.FAKE
    if (flowDesc.dataType == DataType.FAKE) {
      val buf = new Array[Byte](65536)
      var bytesReceived = 0L
      while (bytesReceived < flowDesc.sizeInBytes) {
        val n = tis.read(buf)
        // logInfo("Received " + n + " bytes of " + flowDesc.sizeInBytes)
        if (n == -1) {
          logError("EOF reached after " + bytesReceived + " bytes")
          throw new VarysException("Too few bytes received")
        } else {
          bytesReceived += n
        }
      }
    } else {
      val ois = new ObjectInputStream(tis)
      val resp = ois.readObject.asInstanceOf[Option[Array[Byte]]]
      resp match {
        case Some(byteArr) => {
          logInfo("Received response of " + byteArr.length + " bytes")

          flowDesc.dataType match {
            case DataType.ONDISK => {
              retVal = byteArr
            }

            case DataType.INMEMORY => {
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
    }
    logTrace("Received " + flowDesc.sizeInBytes + " bytes for " + flowDesc + " in " + (now - st) + 
      " milliseconds")

    // Now, notify the local slave
    AkkaUtils.tellActor(slaveActor, CompletedFlow(flowDesc.coflowId, sockLocalPort))
    
    // Close everything
    tis.close
    sock.close
    
    (flowDesc, retVal)
  }
  
  /**
   * Notifies the slave. But everything is done in the client.
   * Blocking call.
   */
  @throws(classOf[VarysException])
  private def handleGet(
      blockId: String, 
      dataType: DataType.DataType, 
      coflowId: String,
      flowDesc: FlowDescription): Array[Byte] = {
    
    val (_, retVal) = getOne(flowDesc)
    retVal
  }
  
  /**
   * Notifies the slave. But everything is done in the client.
   * Blocking call.
   * FIXME: Handles only DataType.FAKE right now.
   */
  @throws(classOf[VarysException])
  private def handleGetMultiple(
      blockIds: Array[String], 
      dataType: DataType.DataType, 
      coflowId: String,
      flowDescs: Array[FlowDescription]) {
    
    throw new VarysException("FIXME: Not supported!!!")

    if (dataType != DataType.FAKE) {
      val tmpM = "handleGetMultiple currently supports only DataType.FAKE"
      logWarning(tmpM)
      throw new VarysException(tmpM)
    }

    // Get 'em!
    val recvLock = new Object()
    var recvFinished = 0

    for (flowDesc <- flowDescs) {
      new Thread("Receive thread for " + flowDesc) {
        override def run() {
          getOne(flowDesc)
          recvLock.synchronized {
            recvFinished += 1
            recvLock.notifyAll()
          }
        }
      }.start()
    }

    recvLock.synchronized {
      while (recvFinished < flowDescs.size) {
        recvLock.wait()
      }
    }
  }

  /**
   * Retrieves data from any of the feasible locations. 
   */
  @throws(classOf[VarysException])
  def getObject[T](objectId: String, coflowId: String, objDesc: ObjectDescription): T = {
    val resp = handleGet(objectId, DataType.INMEMORY, coflowId, objDesc)
    Utils.deserialize[T](resp)
  }
  
  /**
   * Gets a file
   */
  @throws(classOf[VarysException])
  def getFile(fileId: String, coflowId: String, fileDesc: FileDescription): Array[Byte] = {
    handleGet(fileId, DataType.ONDISK, coflowId, fileDesc)
  }
  
  /**
   * Paired get() for putFake. Doesn't return anything, but emulates the retrieval process.
   */
  @throws(classOf[VarysException])
  def getFake(blockId: String, coflowId: String, flowDesc: FlowDescription) {
    handleGet(blockId, DataType.FAKE, coflowId, flowDesc)
  }
  
  /**
   * Paired get() for putFakeMultiple. Doesn't return anything, but emulates the retrieval process.
   */
  @throws(classOf[VarysException])
  def getFakeMultiple(blockIds: Array[String], coflowId: String, flowDescs: Array[FlowDescription]) {
    handleGetMultiple(blockIds, DataType.FAKE, coflowId, flowDescs)
  }

  /**
   * Receive 'howMany' machines with the lowest incoming usage
   */
  def getBestRxMachines(howMany: Int, adjustBytes: Long): Array[String] = {
    waitForMasterRegistration
    val BestRxMachines(bestRxMachines) = AkkaUtils.askActorWithReply[BestRxMachines](
      masterActor,  
      RequestBestRxMachines(howMany, adjustBytes))
    bestRxMachines
  }

  /**
   * Receive the machine with the lowest incoming usage
   */
  def getBestRxMachine(adjustBytes: Long): String = {
    waitForMasterRegistration
    val BestRxMachines(bestRxMachines) = AkkaUtils.askActorWithReply[BestRxMachines](
      masterActor,  
      RequestBestRxMachines(1, adjustBytes))
    bestRxMachines(0)
  }

  /**
   * Receive 'howMany' machines with the lowest outgoing usage
   */
  def getTxMachines(howMany: Int, adjustBytes: Long): Array[String] = {
    waitForMasterRegistration
    val BestTxMachines(bestTxMachines) = AkkaUtils.askActorWithReply[BestTxMachines](
      masterActor,  
      RequestBestTxMachines(howMany, adjustBytes))
    bestTxMachines
  }
  
  /**
   * Receive the machine with the lowest outgoing usage
   */
  def getBestTxMachine(adjustBytes: Long): String = {
    waitForMasterRegistration
    val BestTxMachines(bestTxMachines) = AkkaUtils.askActorWithReply[BestTxMachines](
      masterActor,  
      RequestBestTxMachines(1, adjustBytes))
    bestTxMachines(0)
  }
}
