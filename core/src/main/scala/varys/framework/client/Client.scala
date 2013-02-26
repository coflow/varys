package varys.framework.client

import akka.actor._
import akka.pattern.ask
import akka.util.duration._
import akka.pattern.AskTimeoutException
import akka.remote.RemoteClientLifeCycleEvent
import akka.remote.RemoteClientShutdown
import akka.remote.RemoteClientDisconnected
import akka.actor.Terminated
import akka.dispatch.Await

import varys.framework._
import varys.{VarysException, Logging}
import varys.framework.{RegisterCoflow, RegisteredCoflow}
import varys.framework.master.{Master, CoflowInfo}
import varys.framework.slave.Slave
import varys.util.AkkaUtils
import varys.Utils

private[varys] class Client(
    clientName: String,
    masterUrl: String,
    listener: ClientListener)
  extends Logging {

  val INTERNAL_ASK_TIMEOUT_MS: Int = System.getProperty("varys.framework.ask.wait", "5000").toInt
  val AKKA_RETRY_ATTEMPTS: Int = System.getProperty("varys.akka.num.retries", "3").toInt
  val AKKA_RETRY_INTERVAL_MS: Int = System.getProperty("varys.akka.retry.wait", "3000").toInt

  var actorSystem: ActorSystem = null
  
  var masterActor: ActorRef = null
  val clientRegisterLock = new Object
  
  var slaveUrl: String = null
  var slaveActor: ActorRef = null
  
  var clientId: String = null
  var clientActor: ActorRef = null

  class ClientActor extends Actor with Logging {
    var masterAddress: Address = null
    var alreadyDisconnected = false  // To avoid calling listener.disconnected() multiple times

    override def preStart() {
      logInfo("Connecting to master " + masterUrl)
      try {
        masterActor = context.actorFor(Master.toAkkaUrl(masterUrl))
        masterAddress = masterActor.path.address
        masterActor ! RegisterClient(clientName, Utils.localHostName())
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
      case RegisteredClient(clientId_, slaveUrl_) =>
        clientId = clientId_
        slaveUrl = slaveUrl_
        slaveActor = context.actorFor(Slave.toAkkaUrl(slaveUrl))
        listener.connected(clientId)
        clientRegisterLock.synchronized { clientRegisterLock.notifyAll() }  // Ready to go!
        logInfo("Registered to master. Local slave url = " + slaveUrl)

      case Terminated(actor_) if actor_ == masterActor =>
        logError("Connection to master failed; stopping client")
        markDisconnected()
        context.stop(self)

      case RemoteClientDisconnected(transport, address) if address == masterAddress =>
        logError("Connection to master failed; stopping client")
        markDisconnected()
        context.stop(self)

      case RemoteClientShutdown(transport, address) if address == masterAddress =>
        logError("Connection to master failed; stopping client")
        markDisconnected()
        context.stop(self)

      case StopClient =>
        markDisconnected()
        sender ! true
        context.stop(self)
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
  
  def awaitTermination() { actorSystem.awaitTermination() }
  
  def registerCoflow(coflowDesc: CoflowDescription): String = {
    // Wait until the client has been registered
    while (clientId == null) {
      clientRegisterLock.synchronized { clientRegisterLock.wait() }
    }
    val RegisteredCoflow(coflowId) = askActorWithReply[RegisteredCoflow](masterActor, RegisterCoflow(coflowDesc))
    coflowId
  }
  
  def unregisterCoflow(coflowId: String) {
    while (clientId == null) {
      clientRegisterLock.synchronized { clientRegisterLock.wait() }
    }
    tellActor(masterActor, UnregisterCoflow(coflowId))
  }

  def put() {
    // Store data locally

    // Register with the master
    
  }
  
  def get() {
    // Find location
    
    // Get data
  }
  
  /** Send a one-way message to an actor, to which we expect it to reply with true. */
  private def tellActor(actor: ActorRef, message: Any) {
    if (!askActorWithReply[Boolean](actor, message)) {
      throw new VarysException(actor + " returned false, expected true.")
    }
  }

  /**
   * Send a message to an actor and get its result within a default timeout, or
   * throw a VarysException if this fails.
   */
  private def askActorWithReply[T](actor: ActorRef, message: Any): T = {
    // TODO: Consider removing multiple attempts
    if (actor == null) {
      throw new VarysException("Error sending message as the actor is null " +
        "[message = " + message + "]")
    }
    var attempts = 0
    var lastException: Exception = null
    while (attempts < AKKA_RETRY_ATTEMPTS) {
      attempts += 1
      try {
        val timeout = INTERNAL_ASK_TIMEOUT_MS.millis
        val future = actor.ask(message)(timeout)
        val result = Await.result(future, timeout)
        if (result == null) {
          throw new Exception(actor + " returned null")
        }
        return result.asInstanceOf[T]
      } catch {
        case ie: InterruptedException => throw ie
        case e: Exception =>
          lastException = e
          logWarning("Error sending message to " + actor + " in " + attempts + " attempts", e)
      }
      Thread.sleep(AKKA_RETRY_INTERVAL_MS)
    }

    throw new VarysException(
      "Error sending message to " + actor + " [message = " + message + "]", lastException)
  }
  
}
