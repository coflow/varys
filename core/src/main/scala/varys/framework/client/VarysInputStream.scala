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
 * The VarysInputStream enables Varys on InputStream. 
 * It is implemented as a wrapper on top of another InputStream instance.
 * Currently, works only directly on sockets.
 *
 * It departs from other InputStreams by returning 0 as the number of bytes read.
 * InputStream returns 0 iff he supplied byte array has length zero, whereas VarysInputStream
 * returns zero in the absence of tokens.
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
    val doRead = preRead(1)
    if (doRead) {
      val data = rawStream.read()
      if (data != -1) {
        postRead(1)
      }
      data
    } else {
      0
     }
  }

  override def read(b: Array[Byte]): Int = {
    val doRead = preRead(b.length)
    if (doRead) {
      val readLen = rawStream.read(b)
      if (readLen != -1) {
        postRead(readLen)
      }
      readLen
    } else {
      0
    }
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    val doRead = preRead(len)
    if (doRead) {
      val readLen = rawStream.read(b, off, len)
      if (readLen != -1) {
        postRead(readLen)
      }
      readLen
    } else {
      0
    }
  }

  override def close() {
    VarysInputStream.unregister(visId)
    rawStream.close()
  }

  /**
   * Returns true if this read should happen
   */
  private def preRead(readLen: Long): Boolean = {
    if (bytesRead >= MIN_NOTIFICATION_THRESHOLD) {
      VarysInputStream.getReadToken(readLen)
    } else {
      true
    }
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

  override def toString(): String = {
    "VarysInputStream{" +
      ", bytesRead=" + bytesRead +
      "}";
  }
}

private[client] object VarysInputStream extends Logging {
  val MIN_LOCAL_UPDATE_BYTES = 1048576L

  var actorSystem: ActorSystem = null
  var clientActor: ActorRef = null

  val initCalled = new AtomicBoolean(false)
  
  var coflowId: String = "UNKNOWN"
  var clientName: String = ""

  var slaveActor: ActorRef = null
  var slaveClientId: String = null

  val curVISId = new AtomicInteger(0)
  val activeStreams = new ConcurrentHashMap[Int, VarysInputStream]()

  val messagesBeforeSlaveConnection = new LinkedBlockingQueue[FrameworkMessage]()

  // TODO: Consider using actual bytes, instead of number of requests
  val READ_QUEUE_SIZE = System.getProperty("varys.client.rxQueueSize", "16").toInt
  val tokenQueue = new LinkedBlockingQueue[Object]()
  val reqQueue = new ArrayBlockingQueue[Object](READ_QUEUE_SIZE)

  var slaveChronicle = new VanillaChronicle(HFTUtils.HFT_LOCAL_SLAVE_PATH)
  var slaveAppender = slaveChronicle.createAppender()

  var localChronicle: VanillaChronicle = null
  var localTailer: ExcerptTailer = null

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
   * Returns true if this read should proceed.
   * FIXME: Does it need to be synchronized? 
   */
  def getReadToken(readLen: Long): Boolean = {
    if (slaveClientId != null) {
      val tok = tokenQueue.poll()
      if (tok == null) {
        reqQueue.put(new Object)
        slaveAppender.startExcerpt()
        slaveAppender.writeInt(HFTUtils.GetReadToken)
        slaveAppender.writeUTF(slaveClientId)
        slaveAppender.writeUTF(coflowId)
        slaveAppender.writeLong(readLen)
        slaveAppender.finish()
        false
      } else {
        true
      }
    } else {
      true
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
                  case HFTUtils.ReadToken => {            
                    self ! ReadToken
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

      case ReadToken => {
        tokenQueue.put(new Object)
        reqQueue.take()
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
