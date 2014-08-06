package varys.framework.client

import akka.actor._
import akka.util.duration._
import akka.remote.{RemoteClientLifeCycleEvent, RemoteClientDisconnected, RemoteClientShutdown}

import java.io._
import java.net._
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}
import java.util.concurrent.atomic._

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

import varys.{Logging, Utils, VarysException}
import varys.framework._
import varys.framework.slave.Slave
import varys.util._

/**
 * The VarysInputStream enables Varys on InputStream. 
 * It is implemented as a wrapper on top of another InputStream instance.
 * Currently, works only directly on sockets.
 */
class VarysInputStream(
    val sock: Socket,
    val coflowId: String)
  extends InputStream() with Logging {

  val MIN_NOTIFICATION_THRESHOLD: Long = 
    System.getProperty("varys.client.minNotificationMB", "1").toLong * 1048576L
  var firstNotification = true

  // For InputStream, remote is stored as flow source
  val sIPPort = Utils.getIPPortOfSocketAddress(sock.getRemoteSocketAddress)
  val dIPPort = Utils.getIPPortOfSocketAddress(sock.getLocalSocketAddress)

  // Register with the shared VarysInputStream object
  val visId = VarysInputStream.register(this, coflowId)

  val rawStream = sock.getInputStream

  var bytesRead = 0L
  
  override def read(): Int = {
    preRead(1)
    val data = rawStream.read()
    if (data != -1) {
      postRead(1)
    }
    data
  }

  override def read(b: Array[Byte]): Int = {
    preRead(b.length)
    val readLen = rawStream.read(b)
    if (readLen != -1) {
      postRead(readLen)
    }
    readLen
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    preRead(len)
    val readLen = rawStream.read(b, off, len)
    if (readLen != -1) {
      postRead(readLen)
    }
    readLen
  }

  override def close() {
    VarysInputStream.unregister(visId)
    rawStream.close()
  }

  private def postRead(readLen: Int) {
    bytesRead += readLen
    if (bytesRead > MIN_NOTIFICATION_THRESHOLD) {
      if (firstNotification) {
        VarysInputStream.updateReceivedSoFar(bytesRead)
        firstNotification = false
      } else {
        VarysInputStream.updateReceivedSoFar(readLen)
      }
    }
  }

  private def preRead(readLen: Long) {
    VarysInputStream.getReadToken(readLen)
  }

  override def toString(): String = {
    "VarysInputStream{" +
      ", bytesRead=" + bytesRead +
      "}";
  }
}

private[client] object VarysInputStream extends Logging {
  val LOCAL_SYNC_PERIOD_MILLIS = System.getProperty("varys.framework.localSyncPeriod", "8").toInt
  val MIN_LOCAL_UPDATE_BYTES = 131072L * LOCAL_SYNC_PERIOD_MILLIS

  var actorSystem: ActorSystem = null
  var clientActor: ActorRef = null

  val initCalled = new AtomicBoolean(false)
  
  var coflowId: String = "UNKNOWN"
  var clientName: String = ""

  var slaveActor: ActorRef = null
  val slaveClientRegisterLock = new Object
  
  var slaveClientId: String = null

  val curVISId = new AtomicInteger(0)
  val activeStreams = new ConcurrentHashMap[Int, VarysInputStream]()

  val messagesBeforeSlaveConnection = new LinkedBlockingQueue[FrameworkMessage]()

  val tokenQueue = new LinkedBlockingQueue[Int]()

  private var lastSent = new AtomicLong(0)
  private val receivedSoFar = new AtomicLong(0)
  def updateReceivedSoFar(delta: Long) = synchronized {
    val recvdSoFar = receivedSoFar.addAndGet(delta)
    if (slaveClientId != null && recvdSoFar - lastSent.get > MIN_LOCAL_UPDATE_BYTES) {
      slaveActor ! UpdateCoflowSize(coflowId, recvdSoFar)
      lastSent.set(recvdSoFar)
    }
  }

  /**
   * Blocks until receiving a token to send from local slave
   * FIXME: Does it need to be synchronized? 
   */
  def getReadToken(readLen: Long) {
    if (slaveClientId != null) {
      slaveActor ! GetReadToken(slaveClientId, coflowId, readLen)
      tokenQueue.take()
    }
  }

  private def init(coflowId_ : String) {
    if (!initCalled.getAndSet(true)) {
      coflowId = coflowId_
      clientName = (coflowId + "@" + Utils.localHostName).replaceAll("[^a-zA-Z0-9\\-]+", "")
  
      // Just launch an actor; it will call back into the listener.
      val (actorSystem_, _) = AkkaUtils.createActorSystem(clientName, Utils.localIpAddress, 0)
      actorSystem = actorSystem_
      clientActor = actorSystem.actorOf(Props(new VarysInputStreamActor))
    }
  }

  def register(vis: VarysInputStream, coflowId_ : String): Int = {
    init(coflowId_)
    val visId = curVISId.getAndIncrement()
    activeStreams(visId) = vis
    messagesBeforeSlaveConnection.put(StartedFlow(coflowId, vis.sIPPort, vis.dIPPort))
    visId
  }

  def unregister(visId: Int) {
    val vis = activeStreams(visId)
    messagesBeforeSlaveConnection.put(CompletedFlow(coflowId, vis.sIPPort, vis.dIPPort))
    activeStreams -= visId
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

  class VarysInputStreamActor extends Actor with Logging {
    var slaveUrl: String = "varys://" + Utils.localHostName + ":1607"
    var slaveAddress: Address = null
    var slaveRegStartTime = 0L

    override def preStart() {
      context.system.eventStream.subscribe(self, classOf[RemoteClientLifeCycleEvent])

      logInfo("Connecting to local slave " + slaveUrl)
      slaveRegStartTime = System.currentTimeMillis
      try {
        slaveActor = context.actorFor(Slave.toAkkaUrl(slaveUrl))
        slaveAddress = slaveActor.path.address
        slaveActor ! RegisterSlaveClient(clientName, "", -1)
      } catch {
        case e: Exception =>
          logError("Failed to connect to local slave", e)
          slaveDisconnected()
      }
    }

    override def receive = {
      case RegisteredSlaveClient(clientId_) => {
        slaveClientId = clientId_
        slaveClientRegisterLock.synchronized { 
          slaveClientRegisterLock.notifyAll() 
        }
        logInfo("Registered to local slave in " +  (System.currentTimeMillis - slaveRegStartTime) + 
          " milliseconds.")

        // Thread to periodically update flows to local slave
        context.system.scheduler.schedule(LOCAL_SYNC_PERIOD_MILLIS millis, LOCAL_SYNC_PERIOD_MILLIS millis) {
          val messages = new ListBuffer[FrameworkMessage]()
          messagesBeforeSlaveConnection.drainTo(messages)

          // TODO: Optimize by ignoring coupled Started/Completed messages
          for (m <- messages) {
            slaveActor ! m
          }
        }
      }

      case Terminated(actor_) => {
        if (actor_ == slaveActor) {
          slaveDisconnected()
        }
      }

      case RemoteClientDisconnected(_, address) => {
        if (address == slaveAddress) {
          slaveDisconnected()
        }
      }

      case RemoteClientShutdown(_, address) => {
        if (address == slaveAddress) {
          slaveDisconnected()
        }
      }

      case ReadToken => {
        tokenQueue.put(0)
      }
    }

    // TODO: It would be nice to try to reconnect to the slave, but just shut down for now.
    def slaveDisconnected() {
      val connToSlaveFailedMsg = "Connection to local slave failed. Stopping VarysInputStreamActor."
      logWarning(connToSlaveFailedMsg)
      context.stop(self)
    }

  }
}
