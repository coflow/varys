package varys.framework.client

import akka.actor._
import akka.actor.Terminated
import akka.pattern.ask
import akka.pattern.AskTimeoutException
import akka.remote.{RemotingLifecycleEvent, DisassociatedEvent}

import java.io._
import java.net._

import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

import varys.{Logging, Utils, VarysException}
import varys.framework._
import varys.framework.master.{Master, CoflowInfo}
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
    
  var masterClientId: String = null

  var clientActor: ActorRef = null

  // ExecutionContext for Futures
  implicit val futureExecContext = ExecutionContext.fromExecutor(Utils.newDaemonCachedThreadPool())

  var masterRegStartTime = 0L

  val serverThreadName = "ServerThread for Client@" + Utils.localHostName()

  var clientHost = Utils.localHostName()
  var clientCommPort = -1

  class ClientActor extends Actor with Logging {
    var masterAddress: Address = null
    var slaveAddress: Address = null

    // To avoid calling listener.disconnected() multiple times
    var alreadyDisconnected = false  

    override def preStart() {
      context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
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

    override def receive = {
      
      case RegisterWithMaster =>
        logInfo("Connecting to master " + masterUrl)
        masterRegStartTime = now
        try {
          masterActor = AkkaUtils.getActorRef(Master.toAkkaUrl(masterUrl), context)
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

      case Terminated(actor_) => 
        if (actor_ == masterActor) {
          masterDisconnected()
        } 

      case e: DisassociatedEvent if e.remoteAddress == masterAddress =>
        masterDisconnected()

      case StopClient =>
        markDisconnected()
        sender ! true
        context.stop(self)
        
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
  
  def registerCoflow(coflowDesc: CoflowDescription): Int = {
    registerCoflow(coflowDesc, Array[Int]())
  }
  
  def registerCoflow(coflowDesc: CoflowDescription, parentCoflows: Array[Int]): Int = {
    waitForMasterRegistration

    // Register with the master
    val RegisteredCoflow(coflowId) = AkkaUtils.askActorWithReply[RegisteredCoflow](masterActor, 
      RegisterCoflow(masterClientId, coflowDesc, parentCoflows))
      
    coflowId
  }

  def unregisterCoflow(coflowId: Int) {
    waitForMasterRegistration
    
    // Let the master know
    AkkaUtils.tellActor(masterActor, UnregisterCoflow(coflowId))
    
    // Free local resources
    freeLocalResources(coflowId)
  }

  private def freeLocalResources(coflowId: Int) {
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
