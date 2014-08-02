package varys.framework.client

import akka.actor._
import akka.actor.Terminated
import akka.remote.{RemoteClientLifeCycleEvent, RemoteClientDisconnected, RemoteClientShutdown}

import java.io._
import java.net._
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic._

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

  // Register with the shared VarysOutputStream object
  val visId = VarysOutputStream.register(this, coflowId)

  val rawStream = sock.getOutputStream

  val startTime = System.currentTimeMillis()

  val mBPSLock = new Object

  var maxBytesPerSec: Long = 1048576 * 128
  var bytesWritten = 0L
  var totalSleepTime = 0L

  val SLEEP_DURATION_MS = 50L

  if (maxBytesPerSec < 0) {
    throw new IOException("Bandwidth " + maxBytesPerSec + " is invalid")
  }
  
  override def write(b: Int) = synchronized {
    throttle()
    rawStream.write(b)
    bytesWritten += 1
    VarysOutputStream.updateSentSoFar(1)
  }

  override def write(b: Array[Byte]) = synchronized {
    throttle()
    rawStream.write(b)
    bytesWritten += b.length
    VarysOutputStream.updateSentSoFar(b.length)
  }

  override def write(b: Array[Byte], off: Int, len: Int) = synchronized {
    throttle()
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

  private def throttle() {
    while (maxBytesPerSec <= 0.0) {
      mBPSLock.synchronized {
        logTrace(this + " maxBytesPerSec <= 0.0. Sleeping.")
        mBPSLock.wait()
      }
    }

    // NEVER exceed the specified rate
    while (getBytesPerSec > maxBytesPerSec) {
      try {
        Thread.sleep(SLEEP_DURATION_MS)
        totalSleepTime += SLEEP_DURATION_MS
      } catch {
        case ie: InterruptedException => throw new IOException("Thread aborted", ie)
      }
    }
  }

  def setNewRate(newMaxBitPerSec: Double) {
    maxBytesPerSec = (newMaxBitPerSec / 8).toLong
    mBPSLock.synchronized {
      logTrace(this + " newMaxBitPerSec = " + newMaxBitPerSec)
      mBPSLock.notifyAll()
    }
  }

  def getTotalBytesWritten() = bytesWritten

  def getBytesPerSec(): Long = {
    val elapsed = (System.currentTimeMillis() - startTime) / 1000
    if (elapsed == 0) {
      bytesWritten 
    } else {
      bytesWritten / elapsed
    }
  }

  def getTotalSleepTime() = totalSleepTime

  override def toString(): String = {
    "VarysOutputStream{" +
      ", bytesWritten=" + bytesWritten +
      ", maxBytesPerSec=" + maxBytesPerSec +
      ", bytesPerSec=" + getBytesPerSec +
      ", totalSleepTime=" + totalSleepTime +
      "}";
  }
}

private[client] object VarysOutputStream extends Logging {
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

  private val sentSoFar = new AtomicLong(0)

  def updateSentSoFar(delta: Long) {
    sentSoFar.addAndGet(delta)
  }

  private def init(coflowId: String) {
    if (!initCalled.getAndSet(true)) {
      clientName = (coflowId + "@" + Utils.localHostName).replaceAll("[^a-zA-Z0-9\\-]+", "")
  
      // Just launch an actor; it will call back into the listener.
      val (actorSystem_, _) = AkkaUtils.createActorSystem(clientName, Utils.localIpAddress, 0)
      actorSystem = actorSystem_
      clientActor = actorSystem.actorOf(Props(new VarysOutputStreamActor))
    }
  }

  def register(vis: VarysOutputStream, coflowId: String): Int = {
    init(coflowId)
    val visId = curVISId.getAndIncrement()
    activeStreams(visId) = vis
    visId
  }

  def unregister(visId: Int) {
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
      case RegisteredSlaveClient(clientId_) =>
        slaveClientId = clientId_
        slaveClientRegisterLock.synchronized { 
          slaveClientRegisterLock.notifyAll() 
        }
        logInfo("Registered to local slave in " +  (System.currentTimeMillis - slaveRegStartTime) + 
          " milliseconds.")

      case Terminated(actor_) => 
        if (actor_ == slaveActor) {
          slaveDisconnected()
        }

      case RemoteClientDisconnected(_, address) => 
        if (address == slaveAddress) {
          slaveDisconnected()
        }

      case RemoteClientShutdown(_, address) => 
        if (address == slaveAddress) {
          slaveDisconnected()
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
