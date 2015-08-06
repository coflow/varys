package varys.framework.slave

import akka.actor._
import akka.remote.{DisassociatedEvent, RemotingLifecycleEvent}

import com.google.common.io.Files

import java.io.{File, ObjectInputStream, ObjectOutputStream, IOException}
import java.text.SimpleDateFormat
import java.util.concurrent.atomic._
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}
import java.util.Date
import java.net._

import net.openhft.chronicle.ExcerptTailer
import net.openhft.chronicle.VanillaChronicle

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
      val coflowId: Int,
      var curSize: Long) {

    val flows = new ArrayBuffer[String]()
    
    var lastUpdatedTime = System.currentTimeMillis

    var clientId: String = null

    def updateSize(curSize_ : Long) {
      curSize = curSize_
      lastUpdatedTime = System.currentTimeMillis
    }

    def addFlow(dIP: String) {
      flows += dIP
    }

    def deleteFlow(dIP: String) {
      flows -= dIP
    }

    // Rate measurements
    var lastPrintTime = 0L
    var lastPrintSize = 0L
    var lastPrintRate = 0L
  }

  val HEARTBEAT_SEC = System.getProperty("varys.framework.heartbeat", "1").toInt
  val REMOTE_SYNC_PERIOD_MILLIS = System.getProperty("varys.framework.remoteSyncPeriod", "80").toInt

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

  val coflows = new ConcurrentHashMap[Int, CoflowInfo]()
  val coflowUpdated = new AtomicBoolean(false)
  
  val ccLock = new Object
  var currentCoflowOrder: Array[Int] = null

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
    HFTUtils.cleanWorkDir
    slaveChronicle = new VanillaChronicle(HFTUtils.HFT_LOCAL_SLAVE_PATH)
    slaveTailer = slaveChronicle.createTailer()

    // Thread for reading chronicle input
    val someThread = new Thread(new Runnable() { 
      override def run() {
        while (true) {
          while (slaveTailer.nextIndex) {
            val msgType = slaveTailer.readInt()
            msgType match {
              case HFTUtils.UpdateCoflowSize => {
                val cf = slaveTailer.readInt
                val cs = slaveTailer.readLong
                val rm = slaveTailer.readLong
                self ! UpdateCoflowSize(cf, cs, rm)
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

    // Thread for periodically printing coflow rates
    Utils.scheduleDaemonAtFixedRate(0, HEARTBEAT_SEC * 1000) {
      var totalRate = 0L
      for ((_, c) <- coflows) {
        var rate = (c.curSize - c.lastPrintSize) / (System.currentTimeMillis - c.lastPrintTime) / 128
        rate = (c.lastPrintRate * 0.2 + rate * 0.8).toLong
        totalRate += rate.toLong
        logDebug(c.coflowId + " => " + rate + " Mbps")
        c.lastPrintSize = c.curSize
        c.lastPrintTime = System.currentTimeMillis
        c.lastPrintRate = rate
      }
      logDebug("TOTAL => " + totalRate + " Mbps")
    }     
  }

  override def postStop() {
    webUi.stop()
  }

  def connectToMaster() {
    logInfo("Connecting to master " + masterUrl)
    try {
      master = AkkaUtils.getActorRef(Master.toAkkaUrl(masterUrl), context)
      masterAddress = master.path.address
      master ! RegisterSlave(slaveId, ip, port, webUi.boundPort.get, commPort, publicAddress)
      context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
    } catch {
      case e: Exception =>
        logError("Failed to connect to master", e)
        System.exit(1)
    }
  }

  override def receive = {
    case RegisterSlaveClient(coflowId, clientName, host, commPort) => {
      val currentSender = sender
      val st = now
      logDebug("Registering client %s@%s:%d".format(clientName, host, commPort))
      
      val client = addClient(clientName, host, commPort, currentSender)
      
      if (!coflows.containsKey(coflowId)) {
        coflows(coflowId) = CoflowInfo(coflowId, 0)
      }
      coflows(coflowId).clientId = client.id
      
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
        if (coflows.size > 0 && coflowUpdated.getAndSet(false)) {
          logTrace("Sending LocalCoflows with " + coflows.size + " coflows")
          val activeCoflows = coflows.filter(_._2.flows.size > 0)
          master ! LocalCoflows(slaveId, activeCoflows.map(_._2.coflowId).toArray, 
            activeCoflows.map(_._2.curSize).toArray, activeCoflows.map(_._2.flows.toArray).toArray)
        }
      } 
    }

    case RegisterSlaveFailed(message) => {
      logError("Slave registration failed: " + message)
      System.exit(1)
    }

    case GlobalCoflows(coflowIds, sendTo_) => {
      // Remember latest coflow order
      ccLock.synchronized {
        currentCoflowOrder = coflowIds
      }

      val newSchedule = coflowIds.mkString("-->")
      logDebug("Received GlobalCoflows of size " + coflowIds.size + " " + newSchedule + " [" + 
        sendTo_.mkString(" ") + "]")

      // Stop all
      for ((_, c) <- coflows) {
        if (idToActor.containsKey(c.clientId)) {
          idToActor(c.clientId) ! PauseAll
        } else {
          logTrace("PauseAll: idToActor doesn't contain " + c.clientId)
        }
      }

      // Start some
      var sendTo = sendTo_.toBuffer
      for (c <- coflowIds) {
        if (coflows.containsKey(c)) {
          if (idToActor.containsKey(coflows(c).clientId)) {
            idToActor(coflows(c).clientId) ! StartSome(sendTo.toArray)
            sendTo --= coflows(c).flows
          } else {
            logTrace("StartSome: idToActor doesn't contain " + coflows(c).clientId)
          }
        } else {
          logTrace("StartSome: coflows doesn't contain " + c)
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

    case e: DisassociatedEvent => {
      if (e.remoteAddress == masterAddress) {
        masterDisconnected()
      }
      if (addressToClient.containsKey(e.remoteAddress))  
        removeClient(addressToClient.get(e.remoteAddress))
    }
    
    case RequestSlaveState => {
      sender ! SlaveState(ip, port, slaveId, masterUrl, curRxBps, curTxBps, masterWebUiUrl)
    }
    
    case StartedFlow(coflowId, sIP, dIP) => {
      val currentSender = sender
      logDebug("Received StartedFlow for " + sIP + "-->" + dIP + " of coflow " + coflowId)
      coflows(coflowId).addFlow(dIP)
      coflowUpdated.set(true)
    }

    case CompletedFlow(coflowId, sIP, dIP) => {
      val currentSender = sender
      logDebug("Received CompletedFlow for " + sIP + "-->" + dIP + " of coflow " + coflowId)
      coflows(coflowId).deleteFlow(dIP)
      coflowUpdated.set(true)

      // Prioritize flow from the next coflow until next update
      ccLock.synchronized {
        breakable {
          for (c <- currentCoflowOrder) {
            if (c != coflowId && coflows.containsKey(c) && coflows(c).flows.contains(dIP)) {
              logDebug("Promoted " + sIP + "-->" + dIP + " of coflow " + c)
              idToActor(coflows(c).clientId) ! StartSome(Array(dIP))
              break
            }
          }
        }
      }
    }

    case UpdateCoflowSize(coflowId, curSize_, curRateMbps) => {
      val currentSender = sender
      
      if (coflows.containsKey(coflowId)) {
        coflows(coflowId).updateSize(curSize_)
      } else {
        coflows(coflowId) = CoflowInfo(coflowId, curSize_)
      }
      coflowUpdated.set(true)
    }

    case UnregisteredCoflow(coflowId) => {
      logDebug("Removing " + coflowId)
      coflows -= coflowId
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
    idToAppender(client.id) = (new VanillaChronicle(HFTUtils.createWorkDirPath(client.id))).createAppender()
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
   * Returns an `akka.tcp://...` URL for the Slave actor given a varysUrl `varys://host:ip`. 
   */
  def toAkkaUrl(varysUrl: String): String = {
    varysUrl match {
      case varysUrlRegex(host, port) =>
        "akka.tcp://%s@%s:%s/user/%s".format(systemName, host, port, actorName)
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
