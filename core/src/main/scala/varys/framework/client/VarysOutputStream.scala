package varys.framework.client

import akka.actor._
import akka.util.duration._
import akka.remote.{RemoteClientLifeCycleEvent, RemoteClientDisconnected, RemoteClientShutdown}

import java.io._
import java.net._
import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap, LinkedBlockingQueue}
import java.util.concurrent.atomic._

import net.openhft.chronicle.ExcerptTailer
import net.openhft.chronicle.VanillaChronicle

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

  val MIN_NOTIFICATION_THRESHOLD = 
    System.getProperty("varys.client.individualMinNotificationMB", "1").toLong * 1048576L

  val dIP = Utils.getIPFromSocketAddress(sock.getRemoteSocketAddress)

  // Register with the shared VarysOutputStream object
  val visId = VarysOutputStream.register(this, coflowId)

  val rawStream = sock.getOutputStream

  var bytesWritten = 0L
  val firstNotification = new AtomicBoolean(true)

  val canProceed = new AtomicBoolean(false)
  val canProceedLock = new Object

  override def write(b: Int) = synchronized {
    preWrite()
    rawStream.write(b) 
    postWrite(1)
  }

  override def write(b: Array[Byte]) = synchronized {
    preWrite()
    rawStream.write(b)
    postWrite(b.length)
  }

  override def write(b: Array[Byte], off: Int, len: Int) = synchronized {
    preWrite()
    rawStream.write(b, off, len)
    postWrite(len)
  }

  override def flush() {
    rawStream.flush()
  }

  override def close() {
    // Block until all writes have completed
    VarysOutputStream.unregister(visId)
    rawStream.close()
  }

  /**
   * Wait for order from control after the minimum bytes have been transfered
   */
  private def preWrite() {
    if (bytesWritten >= MIN_NOTIFICATION_THRESHOLD && VarysOutputStream.slaveClientId != null) {
      while (!canProceed.get) {
        canProceedLock.synchronized {
          canProceedLock.wait
        }
      }
    }
  }

  private def postWrite(writeLen: Long) {
    bytesWritten += writeLen

    if (bytesWritten >= MIN_NOTIFICATION_THRESHOLD) {
      if (firstNotification.getAndSet(false)) {
        VarysOutputStream.updateSentSoFar(bytesWritten)
      } else {
        VarysOutputStream.updateSentSoFar(writeLen)
      }
    }
  }

  override def toString(): String = {
    "VarysOutputStream{" +
      ", bytesWritten=" + bytesWritten +
      "}";
  }
}

private[client] object VarysOutputStream extends Logging {
  // Should be equal to the smallest flow size. Hence...
  val MIN_LOCAL_UPDATE_BYTES = 
    System.getProperty("varys.client.combinedMinNotificationMB", "10").toLong * 1048576L

  // Same source address for all VOS
  val sIP = Utils.localHostName

  var actorSystem: ActorSystem = null
  var clientActor: ActorRef = null

  val initCalled = new AtomicBoolean(false)
  
  var coflowId: String = "UNKNOWN"
  var clientName: String = ""

  var slaveActor: ActorRef = null
  var slaveClientId: String = null

  val curVOSId = new AtomicInteger(0)
  val activeStreams = new ConcurrentHashMap[Int, VarysOutputStream]()
  val dstToStream = new ConcurrentHashMap[String, VarysOutputStream]()

  val messagesBeforeSlaveConnection = new LinkedBlockingQueue[FrameworkMessage]()

  var slaveChronicle = new VanillaChronicle(HFTUtils.HFT_LOCAL_SLAVE_PATH)
  var slaveAppender = slaveChronicle.createAppender()

  var localChronicle: VanillaChronicle = null
  var localTailer: ExcerptTailer = null

  private var lastSent = new AtomicLong(0)
  private val sentSoFar = new AtomicLong(0)
  def updateSentSoFar(delta: Long) = synchronized {
    val sent = sentSoFar.addAndGet(delta)
    if (slaveClientId != null && sent - lastSent.get > MIN_LOCAL_UPDATE_BYTES) {
      
      // Send high-frequency local message through HFT
      slaveAppender.startExcerpt()
      slaveAppender.writeInt(HFTUtils.UpdateCoflowSize)
      slaveAppender.writeUTF(coflowId)
      slaveAppender.writeLong(sent)
      slaveAppender.finish()

      lastSent.set(sent)
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

  def register(vos: VarysOutputStream, coflowId_ : String): Int = {
    init(coflowId_)
    val vosId = curVOSId.getAndIncrement()
    activeStreams(vosId) = vos
    dstToStream(vos.dIP) = vos
    if (slaveClientId == null) {
      messagesBeforeSlaveConnection.put(StartedFlow(coflowId, sIP, vos.dIP))
    } else {
      slaveActor ! StartedFlow(coflowId, sIP, vos.dIP)
    }
    vosId
  }

  def unregister(vosId: Int) {
    val vos = activeStreams(vosId)
    if (slaveClientId == null) {
      messagesBeforeSlaveConnection.put(CompletedFlow(coflowId, sIP, vos.dIP))
    } else {
      slaveActor ! CompletedFlow(coflowId, sIP, vos.dIP)
    }
    activeStreams -= vosId
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
        slaveActor ! RegisterSlaveClient(coflowId, clientName, "", -1)
      } catch {
        case e: Exception =>
          logError("Failed to connect to local slave", e)
          slaveDisconnected()
      }
    }

    override def receive = {
      case RegisteredSlaveClient(clientId_) => {
        
        // Send missed updates to local slave
        val messages = new ListBuffer[FrameworkMessage]()
        messagesBeforeSlaveConnection.drainTo(messages)
        
        for (m <- messages) {
          slaveActor ! m
        }
  
        // Chronicle preStart
        localChronicle = new VanillaChronicle(HFTUtils.createWorkDirPath(clientId_))
        localTailer = localChronicle.createTailer()

        // Thread for reading chronicle input
        val someThread = new Thread(new Runnable() { 
          override def run() {
            while (true) {
              while (localTailer.nextIndex) {
                val msgType = localTailer.readInt()
                msgType match {
                  case HFTUtils.StartAll => {            
                    self ! PauseAll
                  }
                  case HFTUtils.PauseAll => {
                    self ! StartAll
                  }
                }
                localTailer.finish
              }
              Thread.sleep(1)
            }
          }
        })
        someThread.setDaemon(true)
        someThread.start()

        slaveClientId = clientId_
        logInfo("Registered to local slave in " +  (System.currentTimeMillis - slaveRegStartTime) + 
          " milliseconds.")
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

      case StartAll => {
        logTrace("Received StartAll")
        for ((_, vos) <- dstToStream) {
          startOne(vos)
        }
      }

      case PauseAll => {
        logTrace("Received PauseAll")
        for ((_, vos) <- dstToStream) {
          vos.canProceed.set(false)
        }
      }

      case StartSome(dsts) => {
        logTrace("Received StartSome")
        for (d <- dsts) {
          startOne(dstToStream(d))
        }
      }

      case PauseSome(dsts) => {
        logTrace("Received PauseSome")
        for (d <- dsts) {
          dstToStream(d).canProceed.set(false)
        }
      }
    }

    private def startOne(vos: VarysOutputStream) {
      vos.canProceed.set(true)
      vos.canProceedLock.synchronized {
        vos.canProceedLock.notifyAll
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
