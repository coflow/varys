package varys.framework.slave

import akka.actor.{ActorRef, Address, Props, Actor, ActorSystem, Terminated}
import akka.util.duration._
import akka.remote.{RemoteClientLifeCycleEvent, RemoteClientShutdown, RemoteClientDisconnected}

import com.google.common.io.Files

import java.io.{File, ObjectInputStream, ObjectOutputStream, IOException}
import java.text.SimpleDateFormat
import java.util.concurrent.atomic._
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}
import java.util.Date
import java.net._

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

    val tokenRequests = new LinkedBlockingQueue[ActorRef]()

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
  val actorToClient = new ConcurrentHashMap[ActorRef, ClientInfo]
  val addressToClient = new ConcurrentHashMap[Address, ClientInfo]

  val coflows = new ConcurrentHashMap[String, CoflowInfo]
  val coflowSizeUpdated = new AtomicBoolean(false)
  val coflowOrder = new ArrayBuffer[CoflowInfo]()

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

    // Thread for periodically removing dead coflows every CLEANUP_INTERVAL_MS of inactivity    
    context.system.scheduler.schedule(CLEANUP_INTERVAL_MS millis, CLEANUP_INTERVAL_MS millis) {
      logTrace("Cleaning up dead coflows")
      val allCoflows = coflows.values.toBuffer.asInstanceOf[ArrayBuffer[CoflowInfo]]
      val toRemove = allCoflows.filter(x => 
        (System.currentTimeMillis - x.lastUpdatedTime) >= CLEANUP_INTERVAL_MS)
      val numToRemove = toRemove.size
      toRemove.foreach(c => coflows -= c.coflowId)
      logTrace("Removed %d dead coflows %s".format(numToRemove, toRemove))
    } 
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
        context.system.scheduler.schedule(0 millis, HEARTBEAT_SEC * 1000 millis) {
          updateNetStats()
          master ! Heartbeat(slaveId, curRxBps, curTxBps)
        }
      }

      // Thread for periodically updating coflow sizes to master
      context.system.scheduler.schedule(REMOTE_SYNC_PERIOD_MILLIS millis, REMOTE_SYNC_PERIOD_MILLIS millis) {
        if (coflows.size > 0 && coflowSizeUpdated.getAndSet(false)) {
          AkkaUtils.tellActor(master, 
            LocalCoflows(slaveId, coflows.map(c => (c._2.coflowId, c._2.curSize)).toArray))
        }
      } 
    }

    case RegisterSlaveFailed(message) => {
      logError("Slave registration failed: " + message)
      System.exit(1)
    }

    case GlobalCoflows(coflowSizes) => {
      coflowOrder.clear
      for ((cf, _) <- coflowSizes) {
        if (coflows.containsKey(cf)) {
          coflowOrder += coflows(cf)
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
      currentSender ! true
    }

    case CompletedFlow(coflowId, sIPPort, dIPPort) => {
      val currentSender = sender
      logDebug("Received CompletedFlow for " + sIPPort + "-->" + dIPPort + " of coflow " + coflowId)
      if (!coflows.containsKey(coflowId)) {
        coflows(coflowId) = CoflowInfo(coflowId, 0)
      }
      coflows(coflowId).deleteFlow(sIPPort, dIPPort)
      currentSender ! true
    }

    case UpdateCoflowSize(coflowId, curSize_) => {
      logDebug("Received UpdateCoflowSize for coflow " + coflowId + " of size " + curSize_)
      if (coflows.containsKey(coflowId)) {
        coflows(coflowId).updateSize(curSize_)
      } else {
        coflows(coflowId) = CoflowInfo(coflowId, curSize_)
      }
      coflowSizeUpdated.set(true)
    }

    case GetWriteToken(coflowId, tokenLen) => {
      // Remember actor that asked for token
      if (coflows.containsKey(coflowId)) {
        coflows(coflowId).tokenRequests.put(sender)
      }
      // Schedule write token
      self ! ProcessWriteToken
    }

    case ProcessWriteToken => {
      var cf: CoflowInfo = null
      breakable {
        coflowOrder.foreach(c => {
          val requester = c.tokenRequests.poll()
          if (requester != null) {
            requester ! true
            break
          }
        })
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
    client
  }

  def removeClient(client: ClientInfo) {
    if (client != null && idToClient.containsValue(client)) {
      logTrace("Removing " + client)
      idToClient.remove(client.id)
      actorToClient -= client.actor
      addressToClient -= client.actor.path.address
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
