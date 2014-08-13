package varys.framework.slave

import akka.actor.{ActorRef, Address, Props, Actor, ActorSystem, Terminated}
import akka.remote.{RemoteClientLifeCycleEvent, RemoteClientShutdown, RemoteClientDisconnected}

import com.google.common.io.Files

import java.io.{File, ObjectInputStream, ObjectOutputStream, IOException}
import java.text.SimpleDateFormat
import java.util.concurrent.atomic._
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}
import java.util.Date
import java.net._

import net.openhft.chronicle.ExcerptTailer
import net.openhft.chronicle.VanillaChronicle
import net.openhft.chronicle.VanillaChronicleConfig

import org.hyperic.sigar.{Sigar, SigarException, NetInterfaceStat}

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.collection.JavaConversions._
import scala.util.control.Breaks._

import varys.framework._
import varys.framework.master.Master
import varys.framework.slave.ui.SlaveWebUI
import varys.{Logging, Utils, VarysException}
import varys.util._

private[varys] class SlaveActor(
    ip: String,
    port: Int,
    webUiPort: Int,
    commPort: Int,
    masterUrl: String,
    workDirPath: String = null)
  extends Actor with Logging {
  
  case class CoflowInfo(
      val coflowId: String,
      var curSize: Long) {

    val flows = new HashSet[(String, String)]()
    
    var lastUpdatedTime = System.currentTimeMillis

    val readTokenRequests = new LinkedBlockingQueue[GetReadToken]()
    val writeTokenRequests = new LinkedBlockingQueue[GetWriteToken]()

    def updateSize(curSize_ : Long) {
      curSize = curSize_
      lastUpdatedTime = System.currentTimeMillis
    }

    def addFlow(sIPPort: String, dIPPort: String) {
      flows += ((sIPPort, dIPPort))
    }

    def deleteFlow(sIPPort: String, dIPPort: String) {
      flows -= ((sIPPort, dIPPort))
    }
  }

  val HEARTBEAT_SEC = System.getProperty("varys.framework.heartbeat", "1").toInt
  val REMOTE_SYNC_PERIOD_MILLIS = System.getProperty("varys.framework.remoteSyncPeriod", "80").toInt
  val CLEANUP_INTERVAL_MS = System.getProperty("varys.slave.coflowReapSec", "60").toInt * 1000

  val LOCAL_SYNC_PERIOD_MILLIS = System.getProperty("varys.framework.localSyncPeriod", "8").toInt  
  val MIN_READ_BYTES  = 131072L * LOCAL_SYNC_PERIOD_MILLIS
  val MIN_WRITE_BYTES = 131072L * LOCAL_SYNC_PERIOD_MILLIS

  val serverThreadName = "ServerThread for Slave@" + Utils.localHostName()

  val DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss")  // For slave IDs

  var master: ActorRef = null
  var masterAddress: Address = null
  
  var masterWebUiUrl : String = ""
  val slaveId = generateSlaveId()
  var varysHome: File = null
  var workDir: File = null
  val publicAddress = {
    val envVar = System.getenv("VARYS_PUBLIC_DNS")
    if (envVar != null) envVar else ip
  }
  var webUi: SlaveWebUI = null

  var nextClientNumber = new AtomicInteger()
  val idToClient = new ConcurrentHashMap[String, ClientInfo]()
  val actorToClient = new ConcurrentHashMap[ActorRef, ClientInfo]()
  val addressToClient = new ConcurrentHashMap[Address, ClientInfo]()
  val idToActor = new ConcurrentHashMap[String, ActorRef]()
  val idToAppender = new ConcurrentHashMap[String, VanillaChronicle.VanillaAppender]()

  val coflows = new ConcurrentHashMap[String, CoflowInfo]()
  val coflowSizeUpdated = new AtomicBoolean(false)
  val coflowOrder = new ArrayBuffer[String]()

  val SLAVE_HFT_PATH = "/tmp/HFT-slave"  
  val config = new VanillaChronicleConfig()
  var slaveChronicle: VanillaChronicle = null
  var slaveTailer: ExcerptTailer = null

  var sigar = new Sigar()
  var lastRxBytes = -1.0
  var lastTxBytes = -1.0
  
  var curRxBps = 0.0
  var curTxBps = 0.0

  private def now() = System.currentTimeMillis

  override def preStart() {
    logInfo("Starting Varys slave %s:%d".format(ip, port))
    varysHome = new File(Option(System.getenv("VARYS_HOME")).getOrElse("."))
    logInfo("Varys home: " + varysHome)
    webUi = new SlaveWebUI(this, workDir, Some(webUiPort))

    webUi.start()
    connectToMaster()

    // Chronicle preStart
    slaveChronicle = new VanillaChronicle(SLAVE_HFT_PATH)
    slaveTailer = slaveChronicle.createTailer()

    // Thread for periodically removing dead coflows every CLEANUP_INTERVAL_MS of inactivity    
    Utils.scheduleDaemonAtFixedRate(CLEANUP_INTERVAL_MS, CLEANUP_INTERVAL_MS) {
      logTrace("Cleaning up dead coflows")
      val allCoflows = coflows.values.toBuffer.asInstanceOf[ArrayBuffer[CoflowInfo]]
      val toRemove = allCoflows.filter(x => 
        (System.currentTimeMillis - x.lastUpdatedTime) >= CLEANUP_INTERVAL_MS)
      val numToRemove = toRemove.size
      toRemove.foreach(c => coflows -= c.coflowId)
      logTrace("Removed %d dead coflows %s".format(numToRemove, toRemove))
    } 

    // Thread for reading chronicle input
    val someThread = new Thread(new Runnable() { 
      override def run() {
        while (true) {
          while (slaveTailer.nextIndex) {
            val msgType = slaveTailer.readInt()
            msgType match {
              case HFTUtils.GetReadToken => {
                val clientId = slaveTailer.readUTF()
                val coflowId = slaveTailer.readUTF()
                val len = slaveTailer.readLong()
                self ! GetReadToken(clientId, coflowId, len)
              }
              case HFTUtils.GetWriteToken => {            
                val clientId = slaveTailer.readUTF()
                val coflowId = slaveTailer.readUTF()
                val len = slaveTailer.readLong()
                self ! GetWriteToken(clientId, coflowId, len)
              }
            }
            slaveTailer.finish
          }
          Thread.sleep(1)
        }
      }
    })
    someThread.setDaemon(true)
    someThread.start()
  }

  override def postStop() {
    webUi.stop()
  }

  def connectToMaster() {
    logInfo("Connecting to master " + masterUrl)
    try {
      master = context.actorFor(Master.toAkkaUrl(masterUrl))
      masterAddress = master.path.address
      master ! RegisterSlave(slaveId, ip, port, webUi.boundPort.get, commPort, publicAddress)
      context.system.eventStream.subscribe(self, classOf[RemoteClientLifeCycleEvent])
      context.watch(master) // Doesn't work with remote actors, but useful for testing
    } catch {
      case e: Exception =>
        logError("Failed to connect to master", e)
        System.exit(1)
    }
  }

  override def receive = {
    case RegisterSlaveClient(clientName, host, commPort) => {
      val currentSender = sender
      val st = now
      logTrace("Registering client %s@%s:%d".format(clientName, host, commPort))
      
      val client = addClient(clientName, host, commPort, currentSender)
      currentSender ! RegisteredSlaveClient(client.id)
      
      logInfo("Registered client " + clientName + " with ID " + client.id + " in " + 
        (now - st) + " milliseconds")
    }

    case RegisteredSlave(url) => {
      masterWebUiUrl = url
      logInfo("Successfully registered with master")

      // Do not send stats by default
      val sendStats = System.getProperty("varys.slave.sendStats", "false").toBoolean
      if (sendStats) {
        // Thread to periodically update last{Rx|Tx}Bytes
        Utils.scheduleDaemonAtFixedRate(0, HEARTBEAT_SEC * 1000) {
          updateNetStats()
          master ! Heartbeat(slaveId, curRxBps, curTxBps)
        }
      }

      // Thread for periodically updating coflow sizes to master
      Utils.scheduleDaemonAtFixedRate(REMOTE_SYNC_PERIOD_MILLIS, REMOTE_SYNC_PERIOD_MILLIS) {
        if (coflows.size > 0 && coflowSizeUpdated.getAndSet(false)) {
          master ! LocalCoflows(slaveId, coflows.map(c => (c._2.coflowId, c._2.curSize)).toArray)
        }
      } 

      // Thread for periodically processing ReadTokens and WriteTokens
      Utils.scheduleDaemonAtFixedRate(LOCAL_SYNC_PERIOD_MILLIS, LOCAL_SYNC_PERIOD_MILLIS) {
        coflowOrder.synchronized {
          breakable {
            var bytesProcessedThisCycle = 0L
            coflowOrder.foreach(c => {
              if (coflows.containsKey(c)) {
                val cf = coflows(c)
                var request = cf.readTokenRequests.poll()
                while(request != null) {
                  logTrace("Sending ReadToken to " + cf)
                  // idToActor(request.clientId) ! ReadToken
                  idToAppender(request.clientId).startExcerpt()
                  idToAppender(request.clientId).writeInt(HFTUtils.ReadToken)
                  idToAppender(request.clientId).finish()
                  bytesProcessedThisCycle += request.readLen
                  if (bytesProcessedThisCycle >= MIN_READ_BYTES) {
                    break
                  }
                  request = cf.readTokenRequests.poll()
                }
              }
            })
          }

          breakable {
            var bytesProcessedThisCycle = 0L
            coflowOrder.foreach(c => {
              if (coflows.containsKey(c)) {
                val cf = coflows(c)
                var request = cf.writeTokenRequests.poll()
                while(request != null) {
                  logTrace("Sending WriteToken to " + cf)
                  // idToActor(request.clientId) ! WriteToken
                  idToAppender(request.clientId).startExcerpt()
                  idToAppender(request.clientId).writeInt(HFTUtils.WriteToken)
                  idToAppender(request.clientId).finish()
                  bytesProcessedThisCycle += request.writeLen
                  if (bytesProcessedThisCycle >= MIN_WRITE_BYTES) {
                    break
                  }
                  request = cf.writeTokenRequests.poll()
                }
              }
            })
          }
        }
      } 
    }

    case RegisterSlaveFailed(message) => {
      logError("Slave registration failed: " + message)
      System.exit(1)
    }

    case GlobalCoflows(coflowSizes) => {
      val newSchedule = coflowSizes.map(_._1).mkString("-->")
      logTrace("Received GlobalCoflows of size " + coflowSizes.length + " " + newSchedule)
      coflowOrder.synchronized {
        coflowOrder.clear
        for ((cf, _) <- coflowSizes) {
          coflowOrder += cf
        }
      }
    }

    case Terminated(actor) => {
      if (actor == master) {
        masterDisconnected()
      }
      if (actorToClient.containsKey(actor))  
        removeClient(actorToClient.get(actor))
    }

    case RemoteClientDisconnected(_, address) => {
      if (address == masterAddress) {
        masterDisconnected()
      }
      if (addressToClient.containsKey(address))  
        removeClient(addressToClient.get(address))
    }

    case RemoteClientShutdown(_, address) => {
      if (address == masterAddress) {
        masterDisconnected()
      }
      if (addressToClient.containsKey(address))  
        removeClient(addressToClient.get(address))
    }

    case RequestSlaveState => {
      sender ! SlaveState(ip, port, slaveId, masterUrl, curRxBps, curTxBps, masterWebUiUrl)
    }
    
    case StartedFlow(coflowId, sIPPort, dIPPort) => {
      val currentSender = sender
      logDebug("Received StartedFlow for " + sIPPort + "-->" + dIPPort + " of coflow " + coflowId)
      
      if (!coflows.containsKey(coflowId)) {
        coflows(coflowId) = CoflowInfo(coflowId, 0)
      }
      coflows(coflowId).addFlow(sIPPort, dIPPort)
    }

    case CompletedFlow(coflowId, sIPPort, dIPPort) => {
      val currentSender = sender
      logDebug("Received CompletedFlow for " + sIPPort + "-->" + dIPPort + " of coflow " + coflowId)
      
      if (!coflows.containsKey(coflowId)) {
        coflows(coflowId) = CoflowInfo(coflowId, 0)
      }
      coflows(coflowId).deleteFlow(sIPPort, dIPPort)
    }

    case UpdateCoflowSize(coflowId, curSize_) => {
      val currentSender = sender
      logDebug("Received UpdateCoflowSize for coflow " + coflowId + " of size " + curSize_)
      
      if (coflows.containsKey(coflowId)) {
        coflows(coflowId).updateSize(curSize_)
      } else {
        coflows(coflowId) = CoflowInfo(coflowId, curSize_)
      }
      coflowSizeUpdated.set(true)
    }

    case GetReadToken(clientId, coflowId, readLen) => {
      val currentSender = sender
      logDebug("Received GetReadToken for coflow " + coflowId + " of read length " + readLen + 
        " for client " + clientId)
      
      if (coflows.containsKey(coflowId)) {
        coflows(coflowId).readTokenRequests.put(GetReadToken(clientId, coflowId, readLen))
      } else {
        idToActor(clientId) ! ReadToken
      }
    }

    case GetWriteToken(clientId, coflowId, writeLen) => {
      val currentSender = sender
      logDebug("Received GetWriteToken for coflow " + coflowId + " of write length " + writeLen + 
        " for client " + clientId)
      
      if (coflows.containsKey(coflowId)) {
        coflows(coflowId).writeTokenRequests.put(GetWriteToken(clientId, coflowId, writeLen))
      } else {
        idToActor(clientId) ! WriteToken
      }
    }
  }

  def masterDisconnected() {
    // TODO: It would be nice to try to reconnect to the master, but just shut down for now.
    // (Note that if reconnecting we would also need to assign IDs differently.)
    logError("Connection to master failed! Shutting down.")
    System.exit(1)
  }

  def addClient(clientName: String, host: String, commPort: Int, actor: ActorRef): ClientInfo = {
    val date = new Date(now)
    val client = new ClientInfo(now, newClientId, host, commPort, date, actor)
    idToClient.put(client.id, client)
    actorToClient(actor) = client
    addressToClient(actor.path.address) = client
    idToActor(client.id) = actor
    idToAppender(client.id) = (new VanillaChronicle("/tmp/HFT-" + client.id)).createAppender()
    client
  }

  def removeClient(client: ClientInfo) {
    if (client != null && idToClient.containsValue(client)) {
      logTrace("Removing " + client)
      idToClient.remove(client.id)
      actorToClient -= client.actor
      addressToClient -= client.actor.path.address
      idToActor -= client.id
      client.markFinished()
    }
  }

  def newClientId(): String = {
    "CLIENT-%06d".format(nextClientNumber.getAndIncrement())
  }

  def generateSlaveId(): String = {
    "slave-%s-%s-%d".format(DATE_FORMAT.format(new Date), ip, port)
  }
  
  /**
   * Update last{Rx|Tx}Bytes before each heartbeat
   * Return the pair (rxBps, txBps)
   */
  def updateNetStats() = {
    var curRxBytes = 0.0;
    var curTxBytes = 0.0;
    
    try {
      val netIfs = sigar.getNetInterfaceList()
      for (i <- 0 until netIfs.length) {
        val net = sigar.getNetInterfaceStat(netIfs(i))
      
        val r = net.getRxBytes()
        if (r >= 0) {
          curRxBytes += r
        }
      
        val t = net.getTxBytes()
        if (t >= 0.0) {
          curTxBytes += t
        }
      }
    } catch {
      case se: SigarException => {
        println(se)
      }
    }
    
    var rxBps = 0.0
    var txBps = 0.0
    if (lastRxBytes >= 0.0 && lastTxBytes >= 0.0) {
      rxBps = (curRxBytes - lastRxBytes) / HEARTBEAT_SEC;
      txBps = (curTxBytes - lastTxBytes) / HEARTBEAT_SEC;
    } 
    
    lastRxBytes = curRxBytes
    lastTxBytes = curTxBytes
    
    curRxBps = rxBps
    curTxBps = txBps

    // FIXME: Sometimes Sigar stops responding, and printing something here brings it back!!!
    // This bug also causes Slave actors to stop responding, which causes the client failures.
    logInfo(rxBps + " " + txBps)
  }
}

private[varys] object Slave {
  private val systemName = "varysSlave"
  private val actorName = "Slave"
  private val varysUrlRegex = "varys://([^:]+):([0-9]+)".r

  def main(argStrings: Array[String]) {
    val args = new SlaveArguments(argStrings)
    val (actorSystem, _) = startSystemAndActor(args.ip, args.port, args.webUiPort, args.commPort,
      args.master, args.workDir)
    actorSystem.awaitTermination()
  }

  /** 
   * Returns an `akka://...` URL for the Master actor given a varysUrl `varys://host:ip`. 
   */
  def toAkkaUrl(varysUrl: String): String = {
    varysUrl match {
      case varysUrlRegex(host, port) =>
        "akka://%s@%s:%s/user/%s".format(systemName, host, port, actorName)
      case _ =>
        throw new VarysException("Invalid master URL: " + varysUrl)
    }
  }

  def startSystemAndActor(
      host: String, 
      port: Int, 
      webUiPort: Int, 
      commPort: Int,
      masterUrl: String, 
      workDir: String, 
      slaveNumber: Option[Int] = None): (ActorSystem, Int) = {
    
    // The LocalVarysCluster runs multiple local varysSlaveX actor systems
    val systemName = "varysSlave" + slaveNumber.map(_.toString).getOrElse("")
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(systemName, host, port)
    val actor = actorSystem.actorOf(Props(new SlaveActor(host, boundPort, webUiPort, commPort,
      masterUrl, workDir)), name = "Slave")
    (actorSystem, boundPort)
  }

}
