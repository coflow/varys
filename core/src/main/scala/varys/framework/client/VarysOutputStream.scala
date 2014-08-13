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

  val MIN_NOTIFICATION_THRESHOLD: Long = 
    System.getProperty("varys.client.minNotificationMB", "1").toLong * 1048576L

  // For OutputStream, local is stored as flow source
  val sIPPort = Utils.getIPPortOfSocketAddress(sock.getLocalSocketAddress)
  val dIPPort = Utils.getIPPortOfSocketAddress(sock.getRemoteSocketAddress)

  // Register with the shared VarysOutputStream object
  val visId = VarysOutputStream.register(this, coflowId)

  val writesInProgress = new AtomicInteger(0)
  val writeCompletionLock = new Object

  val rawStream = sock.getOutputStream

  var bytesWritten = 0L

  override def write(b: Int) = synchronized {
    if (preWrite(Array(b.toByte), 0, 1)) {
      rawStream.write(b) 
    } 
    postWrite(1)
  }

  override def write(b: Array[Byte]) = synchronized {
    if (preWrite(b, 0, b.length)) {
      rawStream.write(b)
    }
    postWrite(b.length)
  }

  override def write(b: Array[Byte], off: Int, len: Int) = synchronized {
    if (preWrite(b, off, len)) {
      rawStream.write(b, off, len)
    }
    postWrite(len)
  }

  override def flush() {
    rawStream.flush()
  }

  override def close() {
    // Block until all writes have completed
    if (writesInProgress.get > 0) {
      writeCompletionLock.synchronized {
        writeCompletionLock.wait()
      }
    }
    VarysOutputStream.unregister(visId)
    rawStream.close()
  }

  /**
   * Returns true if this write should be handled locally
   */
  private def preWrite(b: Array[Byte], off: Int, len: Int): Boolean = {
    if (bytesWritten >= MIN_NOTIFICATION_THRESHOLD) {
      VarysOutputStream.getWriteToken(this, b, off, len)
      false
    } else {
      true
    }
  }

  private def postWrite(writeLen: Long) {
    bytesWritten += writeLen
  }

  override def toString(): String = {
    "VarysOutputStream{" +
      ", bytesWritten=" + bytesWritten +
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
  var slaveClientId: String = null

  val curVISId = new AtomicInteger(0)
  val activeStreams = new ConcurrentHashMap[Int, VarysOutputStream]()

  val messagesBeforeSlaveConnection = new LinkedBlockingQueue[FrameworkMessage]()

  // TODO: Consider using actual bytes, instead of number of requests
  val WRITE_QUEUE_SIZE = System.getProperty("varys.framework.txQueueSize", "8").toInt
  val writeQueue = new ArrayBlockingQueue[(VarysOutputStream, Array[Byte])](WRITE_QUEUE_SIZE)

  var slaveChronicle = new VanillaChronicle(HFTUtils.HFT_LOCAL_SLAVE_PATH)
  var slaveAppender = slaveChronicle.createAppender()

  var localChronicle: VanillaChronicle = null
  var localTailer: ExcerptTailer = null

  /**
   * Blocks until receiving a token to send from local slave
   * FIXME: Does it need to be synchronized? 
   */
  def getWriteToken(vos: VarysOutputStream, srcBytes: Array[Byte], off: Int, len: Int) {
    if (slaveClientId != null) {
      // Ask for token
      slaveAppender.startExcerpt()
      slaveAppender.writeInt(HFTUtils.GetWriteToken)
      slaveAppender.writeUTF(slaveClientId)
      slaveAppender.writeUTF(coflowId)
      slaveAppender.writeLong(len)
      slaveAppender.finish()

      // Store to be processed later
      val dstBytes = Array.ofDim[Byte](len)
      System.arraycopy(srcBytes, off, dstBytes, 0, len)
      writeQueue.put((vos, dstBytes))

      // Remember how many writes are yet to be processed
      vos.writesInProgress.incrementAndGet()
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
                  case HFTUtils.WriteToken => {            
                    self ! WriteToken
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

      case WriteToken => {
        val (vos, bytes) = writeQueue.take()
        vos.rawStream.write(bytes)
        val numWriters = vos.writesInProgress.decrementAndGet()
        if (numWriters == 0)
          vos.writeCompletionLock.synchronized {
            vos.writeCompletionLock.notifyAll()
          }
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
