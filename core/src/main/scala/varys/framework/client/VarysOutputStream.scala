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
 * The VarysOutputStream enables Varys on OutputStream. 
 * It is implemented as a wrapper on top of another OutputStream instance.
 * Currently, works only directly on sockets.
 */
class VarysOutputStream(
    val sock: Socket,
    val coflowId: String)
  extends OutputStream() with Logging {

  // For OutputStream, local is stored as flow source
  val sIPPort = Utils.getIPPortOfSocketAddress(sock.getLocalSocketAddress)
  val dIPPort = Utils.getIPPortOfSocketAddress(sock.getRemoteSocketAddress)

  // Register with the shared VarysOutputStream object
  val visId = VarysOutputStream.register(this, coflowId)

  val rawStream = sock.getOutputStream

  var bytesWritten = 0L

  override def write(b: Int) = synchronized {
    preWrite(1)
    rawStream.write(b)
    bytesWritten += 1
    VarysOutputStream.updateSentSoFar(1)
  }

  override def write(b: Array[Byte]) = synchronized {
    preWrite(b.length)
    rawStream.write(b)
    bytesWritten += b.length
    VarysOutputStream.updateSentSoFar(b.length)
  }

  override def write(b: Array[Byte], off: Int, len: Int) = synchronized {
    preWrite(len)
    rawStream.write(b, off, len)
    bytesWritten += len
    VarysOutputStream.updateSentSoFar(len)
  }

  override def flush() {
    rawStream.flush()
  }

  override def close() {
    VarysOutputStream.unregister(visId)
    rawStream.close()
  }

  private def preWrite(writeLen: Long) {
    VarysOutputStream.getWriteToken(dIPPort, writeLen)
  }

  override def toString(): String = {
    "VarysOutputStream{" +
      ", bytesWritten=" + bytesWritten +
      "}";
  }
}

private[client] object VarysOutputStream extends Logging {
  val LOCAL_SYNC_PERIOD_MILLIS = System.getProperty("varys.framework.localSyncPeriod", "8").toInt

  var actorSystem: ActorSystem = null
  var clientActor: ActorRef = null

  val initCalled = new AtomicBoolean(false)
  
  var coflowId: String = "UNKNOWN"
  var clientName: String = ""

  var slaveActor: ActorRef = null
  val slaveClientRegisterLock = new Object
  
  var slaveClientId: String = null

  val curVISId = new AtomicInteger(0)
  val activeStreams = new ConcurrentHashMap[Int, VarysOutputStream]()

  val messagesBeforeSlaveConnection = new LinkedBlockingQueue[FrameworkMessage]()

  val tokenQueue = new LinkedBlockingQueue[Int]()

  private val sentSoFar = new AtomicLong(0)

  def updateSentSoFar(delta: Long) {
    sentSoFar.addAndGet(delta)
  }

  /**
   * Blocks until receiving a token to send from local slave
   * FIXME: Does it need to be synchronized? 
   */
  def getWriteToken(flowDst: String, writeLen: Long) {
    if (slaveClientId != null) {
      slaveActor ! GetWriteToken(slaveClientId, coflowId, writeLen)
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
      clientActor = actorSystem.actorOf(Props(new VarysOutputStreamActor))
    }
  }

  def register(vis: VarysOutputStream, coflowId_ : String): Int = {
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

  class VarysOutputStreamActor extends Actor with Logging {
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

      case WriteToken => {
        tokenQueue.put(0)
      }
    }

    // TODO: It would be nice to try to reconnect to the slave, but just shut down for now.
    def slaveDisconnected() {
      val connToSlaveFailedMsg = "Connection to local slave failed. Stopping VarysOutputStreamActor."
      logWarning(connToSlaveFailedMsg)
      context.stop(self)
    }

  }
}
