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

import varys.{VarysException, Logging}
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

  val flowDescToTIS = new HashMap[FlowDescription, ThrottledInputStream]

  // FIXME: Handle ServerSocket
  var serverSocket: ServerSocket = new ServerSocket(0)

  var clientHost = Utils.localHostName()
  var clientCommPort = serverSocket.getLocalPort

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
          if (flowDescToTIS.contains(flowDesc)) {
            flowDescToTIS(flowDesc).updateRate(newBPS)
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
  }

  /**
   * Makes data available for retrieval, and notifies local slave, which will register it with the master.
   * FIXME: Non-blocking for FAKE and ONDISK FlowTypes. Blocking for in-memory.
   */
  private def handlePut(flowDesc: FlowDescription) {
    // Notify the slave, which will notify the master
    AkkaUtils.tellActor(slaveActor, AddFlow(flowDesc))
    
    // TODO: Block; non-blocking for other FlowTypes
    if (flowDesc.flowType == FlowType.INMEMORY) {
      
    } 
  }

  /**
   * Puts any data structure
   */
  def put() {
    0L
  }
  
  /**
   * Puts a local file
   */
  def putFile(fileId: String, pathToFile: String, coflowId: String, size: Long, numReceivers: Int) {
    val desc = new FileDescription(fileId, pathToFile, coflowId, FlowType.ONDISK, size, numReceivers, 
      clientHost, clientCommPort)
    handlePut(desc)
  }
  
  /**
   * Emulates the process without having to actually put anything
   */
  def putFake(blockId: String, coflowId: String, size: Long, numReceivers: Int) {
    val desc = new FlowDescription(blockId, coflowId, FlowType.FAKE, size, numReceivers, 
      clientHost, clientCommPort)
    handlePut(desc)
  }
  
  /**
   * Notifies the master and the slave. But everything is done in the client
   * 
   */
  private def handleGet(blockId: String, flowType: FlowType.FlowType, coflowId: String) {
    // Notify master and retrieve the FlowDescription in response
    val GotFlowDesc(flowDesc) = AkkaUtils.askActorWithReply[GotFlowDesc](masterActor, 
      GetFlow(blockId, coflowId, clientId, slaveId))
    
    // Notify local slave
    AkkaUtils.tellActor(slaveActor, GetFlow(blockId, coflowId, clientId, slaveId, flowDesc))
    
    // Add to the queue
    // logInfo("Adding " + flowDesc + " to the Q")
    // val targetConManId = new ConnectionManagerId(flowDesc.originHost, flowDesc.originCommPort)
    // getReQ.put(GetRequest(flowDesc, targetConManId))
    
    val sock = new Socket(flowDesc.originHost, flowDesc.originCommPort)
    val oos = new ObjectOutputStream(sock.getOutputStream)
    oos.flush
    val tis = new ThrottledInputStream(sock.getInputStream)
    val ois = new ObjectInputStream(tis)
    
    flowDescToTIS(flowDesc) = tis
    
    oos.writeObject(GetRequest(flowDesc))
    oos.flush
    
    val resp = ois.readObject.asInstanceOf[Option[Array[Byte]]]
    resp match {
      case Some(byteArr) => {
        logInfo("Received response of " + byteArr.length + " bytes")
        
        flowType match {
          case FlowType.FAKE => {
            // Throw away
          }

          case FlowType.ONDISK => {
            // TODO: Write to disk or something else
          }

          case FlowType.INMEMORY => {
            // TODO: Do something
          }

          case _ => {
            logError("Invalid FlowType!")
            throw new VarysException("Invalid FlowType!")
          }
        }
      }
      case None => {
        logError("Nothing received!")
        throw new VarysException("Invalid FlowType!")
      }
    }
    
    ois.close
    oos.close
    sock.close
  }
  
  /**
   * Retrieves data from any of the feasible locations. 
   * Blocking call.
   */
  def get() {
    
  }
  
  /**
   * Gets a file
   */
  def getFile(fileId: String, coflowId: String) {
    handleGet(fileId, FlowType.ONDISK, coflowId)
  }
  
  /**
   * Paired get() for putFake. Doesn't return anything, but emulates the retrieval process.
   */
  def getFake(blockId: String, coflowId: String) {
    handleGet(blockId, FlowType.FAKE, coflowId)
  }
  
  def delete(flowId: String, coflowId: String) {
    AkkaUtils.tellActor(slaveActor, DeleteFlow(flowId, coflowId))
  }

}
