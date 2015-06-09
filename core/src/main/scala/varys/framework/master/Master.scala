package varys.framework.master

import akka.actor._
import akka.remote.{DisassociatedEvent, RemotingLifecycleEvent}
import akka.routing._

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.atomic._
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.collection.JavaConversions._
import scala.concurrent.{Future, ExecutionContext}

import varys.framework._
import varys.framework.master.ui.MasterWebUI
import varys.framework.scheduler.DarkScheduler
import varys.{Logging, Utils, VarysException}
import varys.util.{AkkaUtils, SlaveToBpsMap}

private[varys] class Master(
    systemName:String, 
    actorName: String, 
    host: String, 
    port: Int, 
    webUiPort: Int) 
  extends Logging {
  
  val MAX_DEPTH = System.getProperty("varys.framework.maxDagDepth", "100").toInt

  val NUM_MASTER_INSTANCES = System.getProperty("varys.master.numInstances", "1").toInt
  val DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss")  // For coflow IDs
  val SLAVE_TIMEOUT = System.getProperty("varys.slave.timeout", "60").toLong * 1000
  
  val REMOTE_SYNC_PERIOD_MILLIS = System.getProperty("varys.framework.remoteSyncPeriod", "80").toInt

  val idToSlave = new ConcurrentHashMap[String, SlaveInfo]()
  val idToSlaveActor = new ConcurrentHashMap[String, ActorRef]()
  val actorToSlave = new ConcurrentHashMap[ActorRef, SlaveInfo]
  val addressToSlave = new ConcurrentHashMap[Address, SlaveInfo]
  val hostToSlave = new ConcurrentHashMap[String, SlaveInfo]

  val idToRxBps = new SlaveToBpsMap
  val idToTxBps = new SlaveToBpsMap

  var nextCoflowNumber = new AtomicInteger()
  val idToCoflow = new ConcurrentHashMap[Int, CoflowInfo]()
  val completedCoflows = new ArrayBuffer[CoflowInfo]

  var nextClientNumber = new AtomicInteger()
  val idToClient = new ConcurrentHashMap[String, ClientInfo]()
  val actorToClient = new ConcurrentHashMap[ActorRef, ClientInfo]
  val addressToClient = new ConcurrentHashMap[Address, ClientInfo]

  val webUiStarted = new AtomicBoolean(false)

  // ExecutionContext for Futures
  implicit val futureExecContext = ExecutionContext.fromExecutor(Utils.newDaemonCachedThreadPool())

  private def now() = System.currentTimeMillis
  
  def start(): (ActorSystem, Int) = {
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(systemName, host, port)
    val actor = actorSystem.actorOf(
      Props(new MasterActor(host, boundPort, webUiPort)).withRouter(
        RoundRobinRouter(nrOfInstances = NUM_MASTER_INSTANCES)), 
      name = actorName)
    (actorSystem, boundPort)
  }
  
  private[varys] class MasterActor(
      ip: String, 
      port: Int, 
      webUiPort: Int) 
    extends Actor with Logging {

    val webUi = new MasterWebUI(self, webUiPort)

    val masterPublicAddress = {
      val envVar = System.getenv("VARYS_PUBLIC_DNS")
      if (envVar != null) envVar else ip
    }

    override def preStart() {
      logInfo("Starting Varys master at varys://" + ip + ":" + port)
      // Listen for remote client disconnection events, since they don't go through Akka's watch()
      context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
      if (!webUiStarted.getAndSet(true)) {
        webUi.start()
      }

      // Thread to watch out for dead slaves
      // Utils.scheduleDaemonAtFixedRate(0, SLAVE_TIMEOUT) {
      //   self ! CheckForSlaveTimeOut
      // }

      // Thread to periodically update global coflow sizes to all slaves
      Utils.scheduleDaemonAtFixedRate(0, REMOTE_SYNC_PERIOD_MILLIS) {
        self ! SyncSlaves
      }

    }

    override def postStop() {
      webUi.stop()
    }

    override def receive = {
      case RegisterSlave(id, host, slavePort, slave_webUiPort, slave_commPort, publicAddress) => {
        val currentSender = sender
        logInfo("Registering slave %s:%d".format(host, slavePort))
        if (idToSlave.containsKey(id)) {
          currentSender ! RegisterSlaveFailed("Duplicate slave ID")
        } else {
          addSlave(
            id, 
            host, 
            slavePort, 
            slave_webUiPort, 
            slave_commPort, 
            publicAddress, 
            currentSender)

          // Wait for webUi to bind. Needed when NUM_MASTER_INSTANCES > 1.
          while (webUi.boundPort == None) {
            Thread.sleep(100)
          }
          
          currentSender ! RegisteredSlave("http://" + masterPublicAddress + ":" + webUi.boundPort.get)
        }
      }

      case RegisterMasterClient(clientName, host, commPort) => {
        val currentSender = sender
        val st = now
        logDebug("Registering client %s@%s:%d".format(clientName, host, commPort))
        
        if (hostToSlave.containsKey(host)) {
          val client = addClient(clientName, host, commPort, currentSender)

          val slave = hostToSlave(host)
          currentSender ! RegisteredMasterClient(
            client.id, 
            slave.id, 
            "varys://" + slave.host + ":" + slave.port)
          
          logInfo("Registered client " + clientName + " with ID " + client.id + " in " + 
            (now - st) + " milliseconds")
        } else {
          currentSender ! RegisterClientFailed("No Varys slave at " + host)
        }
      }

      case RegisterCoflow(clientId, description, parentCoflows) => {
        val currentSender = sender
        val st = now
        logDebug("Registering coflow " + description.name)

        val client = idToClient.get(clientId)
        if (client == null) {
          currentSender ! RegisterCoflowFailed("Invalid clientId " + clientId)
        } else {
          val coflow = addCoflow(client, description, parentCoflows, currentSender)

          currentSender ! RegisteredCoflow(coflow.id)
          logInfo("Registered coflow " + description.name + " with ID " + coflow.id + " in " + 
            (now - st) + " milliseconds")
        }
      }

      case UnregisterCoflow(coflowId) => {
        removeCoflow(idToCoflow.get(coflowId))
        sender ! true

        // Let all slaves know without blocking here
        self ! UnregisteredCoflow(coflowId)
      }

      case UnregisteredCoflow(coflowId) => {
        for (slaveActor <- actorToSlave.keys) {
          slaveActor ! UnregisteredCoflow(coflowId)
        }
      }

      case Heartbeat(slaveId, newRxBps, newTxBps) => {
        val slaveInfo = idToSlave.get(slaveId)
        if (slaveInfo != null) {
          slaveInfo.updateNetworkStats(newRxBps, newTxBps)
          slaveInfo.lastHeartbeat = System.currentTimeMillis()

          idToRxBps.updateNetworkStats(slaveId, newRxBps)
          idToTxBps.updateNetworkStats(slaveId, newTxBps)
        } else {
          logWarning("Got heartbeat from unregistered slave " + slaveId)
        }
      }

      case Terminated(actor) => {
        // The disconnected actor could've been a slave or a client; remove accordingly. 
        // Coflow termination is handled explicitly through UnregisterCoflow or when its client dies
        if (actorToSlave.containsKey(actor))
          removeSlave(actorToSlave.get(actor))
        if (actorToClient.containsKey(actor))  
          removeClient(actorToClient.get(actor))
      }

      case e: DisassociatedEvent => {
        // The disconnected actor could've been a slave or a client; remove accordingly. 
        // Coflow termination is handled explicitly through UnregisterCoflow or when its client dies
        if (addressToSlave.containsKey(e.remoteAddress))
          removeSlave(addressToSlave.get(e.remoteAddress))
        if (addressToClient.containsKey(e.remoteAddress))  
          removeClient(addressToClient.get(e.remoteAddress))
      }

      case RequestMasterState => {
        sender ! MasterState(
          ip, 
          port, 
          idToSlave.values.toSeq.toArray, 
          idToCoflow.values.toSeq.toArray, 
          completedCoflows.toArray, 
          idToClient.values.toSeq.toArray)
      }

      case CheckForSlaveTimeOut => {
        timeOutDeadSlaves()
      }

      case SyncSlaves => {
        mergeAllAndSyncSlaves()
      }

      case RequestWebUIPort => {
        sender ! WebUIPortResponse(webUi.boundPort.getOrElse(-1))
      }

      case LocalCoflows(slaveId, coflowIds, sizes, flows) => {
        logTrace("Received LocalCoflows from " + slaveId + " with " + coflowIds.size + " coflows")
        idToSlave.get(slaveId).updateCoflows(coflowIds, sizes, flows)
      }

      case RequestBestRxMachines(howMany, bytes) => {
        sender ! BestRxMachines(idToRxBps.getTopN(
          howMany, bytes).toArray.map(x => idToSlave.get(x).host))
      }

      case RequestBestTxMachines(howMany, bytes) => {
        sender ! BestTxMachines(idToTxBps.getTopN(
          howMany, bytes).toArray.map(x => idToSlave.get(x).host))
      }
    }

    def addSlave(
        id: String, 
        host: String, 
        port: Int, 
        webUiPort: Int, 
        commPort: Int,
        publicAddress: String, 
        actor: ActorRef): SlaveInfo = {
      
      // There may be one or more refs to dead slaves on this same node with 
      // different IDs; remove them.
      idToSlave.values.filter(
        w => (w.host == host) && (w.state == SlaveState.DEAD)).foreach(idToSlave.values.remove(_))
      
      val slave = new SlaveInfo(id, host, port, actor, webUiPort, commPort, publicAddress)
      idToSlave.put(slave.id, slave)
      idToSlaveActor.put(slave.id, actor)
      actorToSlave(actor) = slave
      addressToSlave(actor.path.address) = slave
      hostToSlave(slave.host) = slave
      slave
    }

    def removeSlave(slave: SlaveInfo) {
      slave.setState(SlaveState.DEAD)
      logError("Removing " + slave)
      // Do not remove from idToSlave so that we remember DEAD slaves
      idToSlaveActor -= slave.id
      actorToSlave -= slave.actor
      addressToSlave -= slave.actor.path.address
      hostToSlave -= slave.host
    }

    def addClient(clientName: String, host: String, commPort: Int, actor: ActorRef): ClientInfo = {
      val date = new Date(now)
      val client = new ClientInfo(now, newClientId(date), host, commPort, date, actor)
      idToClient.put(client.id, client)
      actorToClient(actor) = client
      addressToClient(actor.path.address) = client
      client
    }

    def removeClient(client: ClientInfo) {
      if (client != null && idToClient.containsValue(client)) {
        logDebug("Removing " + client)
        idToClient.remove(client.id)
        actorToClient -= client.actor
        addressToClient -= client.actor.path.address
        client.markFinished()
      }
    }

    def addCoflow(
        client: ClientInfo, 
        desc: CoflowDescription, 
        parentCoflows: Array[Int], 
        actor: ActorRef): CoflowInfo = {

      val now = System.currentTimeMillis()
      val date = new Date(now)
      val coflow = new CoflowInfo(now, newCoflowId(parentCoflows), desc, client, date, actor)

      idToCoflow.put(coflow.id, coflow)
      DarkScheduler.addCoflow(coflow.id)

      // Update its parent client
      client.addCoflow(coflow)

      coflow
    }

    // TODO: Let all involved clients know so that they can free up local resources
    def removeCoflow(coflow: CoflowInfo) {
      removeCoflow(coflow, CoflowState.FINISHED)
    }

    def removeCoflow(coflow: CoflowInfo, endState: CoflowState.Value) {
      if (coflow != null && idToCoflow.containsValue(coflow)) {
        idToCoflow.remove(coflow.id)
        DarkScheduler.deleteCoflow(coflow.id)

        completedCoflows += coflow  // Remember it in our history
        coflow.markFinished(endState)
        logInfo("Removing " + coflow)
      }
    }

    /** 
     * Generate a new coflow ID given a coflow's submission date 
     */
    def newCoflowId(parentCoflows: Array[Int]): Int = {
      if (parentCoflows == null || parentCoflows.size == 0) {
        nextCoflowNumber.getAndIncrement() * MAX_DEPTH
      } else {
        val maxCoflowId = parentCoflows.reduceLeft(math.max(_, _))
        maxCoflowId + 1
      }
    }

    /** 
     * Generate a new client ID given a client's connection date 
     */
    def newClientId(submitDate: Date): String = {
      "CLIENT-%06d".format(nextClientNumber.getAndIncrement())
    }

    /** 
     * Check for, and remove, any timed-out slaves 
     */
    def timeOutDeadSlaves() {
      // Copy the slaves into an array so we don't modify the hashset while iterating through it
      val expirationTime = System.currentTimeMillis() - SLAVE_TIMEOUT
      val toRemove = idToSlave.values.filter(_.lastHeartbeat < expirationTime).toArray
      for (slave <- toRemove) {
        logWarning("Removing slave %s because we got no heartbeat in %d seconds".format(
          slave.id, SLAVE_TIMEOUT))
        removeSlave(slave)
      }
    }

    /*
     * Combine current information from each slave and send it out
     */
    def mergeAllAndSyncSlaves() {
      var st = now

      // Update scheduler
      DarkScheduler.updateCoflowSizes(idToSlave)
      val step1Dur = now - st

      // Schedule
      st = now
      DarkScheduler.updateCoflowOrder()
      val step2Dur = now - st

      // Send out if the schedule has changed
      var sentSomething = false
      st = now
      val (slaveAllocs, arrCoflows) = DarkScheduler.getSchedule(idToSlave.keys.toSeq.toArray)
      val newOrder = arrCoflows.mkString("->")
      for ((slaveId, sendTo) <- slaveAllocs) {
        if (!idToSlave(slaveId).sameAsLastSchedule(newOrder, sendTo)) {
          logDebug("Sending new schedule to " + slaveId + " => " + sendTo.mkString("|") + 
            " => " + newOrder)
          idToSlaveActor(slaveId) ! GlobalCoflows(arrCoflows, sendTo.toArray)
          sentSomething = true
        }
      }
      val step3Dur = now - st

      if (sentSomething) {
        logInfo("MERGE_AND_SYNC in " + (step1Dur + step2Dur + step3Dur) + " = (" + step1Dur + "+" + 
          step2Dur + "+" + step3Dur + ") milliseconds")
      }
    }
  }
}

private[varys] object Master {
  private val systemName = "varysMaster"
  private val actorName = "Master"
  private val varysUrlRegex = "varys://([^:]+):([0-9]+)".r

  def main(argStrings: Array[String]) {
    val args = new MasterArguments(argStrings)
    val masterObj = new Master(systemName, actorName, args.ip, args.port, args.webUiPort)
    val (actorSystem, _) = masterObj.start()
    actorSystem.awaitTermination()
  }

  /** 
   * Returns an `akka.tcp://...` URL for the Master actor given a varysUrl `varys://host:ip`. 
   */
  def toAkkaUrl(varysUrl: String): String = {
    varysUrl match {
      case varysUrlRegex(host, port) =>
        "akka.tcp://%s@%s:%s/user/%s".format(systemName, host, port, actorName)
      case _ =>
        throw new VarysException("Invalid master URL: " + varysUrl)
    }
  }

}
