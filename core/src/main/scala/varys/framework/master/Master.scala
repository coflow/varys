package varys.framework.master

import akka.actor._
import akka.actor.Terminated
import akka.remote.{RemoteClientLifeCycleEvent, RemoteClientDisconnected, RemoteClientShutdown}
import akka.util.duration._
import akka.dispatch._

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.collection.JavaConversions._

import varys.framework._
import varys.{Logging, VarysException, Utils}
import varys.util.AkkaUtils

private[varys] class MasterActor(ip: String, port: Int, webUiPort: Int) extends Actor with Logging {
  val DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss")  // For coflow IDs
  val SLAVE_TIMEOUT = System.getProperty("varys.slave.timeout", "60").toLong * 1000
  val NIC_BPS = 1024 * 1048576
  val SCHEDULE_FREQ = System.getProperty("varys.master.schedulerIntervalMillis", "100").toLong

  // Keeps track of when the scheduler last ran
  var lastScheduled = System.currentTimeMillis

  val idToSlave = new ConcurrentHashMap[String, SlaveInfo]()
  val actorToSlave = new HashMap[ActorRef, SlaveInfo]
  val addressToSlave = new HashMap[Address, SlaveInfo]
  val hostToSlave = new HashMap[String, SlaveInfo]

  val idToRxBps = new SlaveToBpsMap
  val idToTxBps = new SlaveToBpsMap

  var nextCoflowNumber = new AtomicInteger()
  val idToCoflow = new ConcurrentHashMap[String, CoflowInfo]()
  val completedCoflows = new ArrayBuffer[CoflowInfo]

  var nextClientNumber = new AtomicInteger()
  val idToClient = new ConcurrentHashMap[String, ClientInfo]()
  val actorToClient = new HashMap[ActorRef, ClientInfo]
  val addressToClient = new HashMap[Address, ClientInfo]
  val completedClients = new ArrayBuffer[ClientInfo]

  // ExecutionContext for Futures
  implicit val futureExecContext = ExecutionContext.fromExecutor(Utils.newDaemonCachedThreadPool())
  
  val masterPublicAddress = {
    val envVar = System.getenv("VARYS_PUBLIC_DNS")
    if (envVar != null) envVar else ip
  }

  override def preStart() {
    logInfo("Starting Varys master at varys://" + ip + ":" + port)
    // Listen for remote client disconnection events, since they don't go through Akka's watch()
    context.system.eventStream.subscribe(self, classOf[RemoteClientLifeCycleEvent])
    startWebUi()
    // context.system.scheduler.schedule(0 millis, SLAVE_TIMEOUT millis)(timeOutDeadSlaves())
  }

  def startWebUi() {
    val webUi = new MasterWebUI(context.system, self)
    try {
      AkkaUtils.startSprayServer(context.system, "0.0.0.0", webUiPort, webUi.handler)
    } catch {
      case e: Exception =>
        logError("Failed to create web UI", e)
        System.exit(1)
    }
  }

  override def receive = {
    case RegisterSlave(id, host, slavePort, slave_webUiPort, slave_commPort, publicAddress) => {
      logInfo("Registering slave %s:%d".format(host, slavePort))
      if (idToSlave.containsKey(id)) {
        sender ! RegisterSlaveFailed("Duplicate slave ID")
      } else {
        val currentSender = sender
        addSlave(id, host, slavePort, slave_webUiPort, slave_commPort, publicAddress, currentSender)
        context.watch(sender)  // This doesn't work with remote actors but helps for testing
        sender ! RegisteredSlave("http://" + masterPublicAddress + ":" + webUiPort)
      }
    }

    case RegisterClient(clientName, host, commPort) => {
      logInfo("Registering client %s@%s:%d".format(clientName, host, commPort))
      if (hostToSlave.contains(host)) {
        val currentSender = sender
        val client = addClient(clientName, host, commPort, currentSender)
        logDebug("Registered client " + clientName + " with ID " + client.id)
        context.watch(sender)  // This doesn't work with remote actors but helps for testing
        val slave = hostToSlave(host)
        sender ! RegisteredClient(client.id, slave.id, "varys://" + slave.host + ":" + slave.port)
      } else {
        sender ! RegisterClientFailed("No Varys slave at " + host)
      }
    }

    case RegisterCoflow(clientId, description) => {
      // clientId will always be in clients
      val client = idToClient.get(clientId)
      assert(client != null)
      
      logDebug("Registering coflow " + description.name)
      val currentSender = sender
      val coflow = addCoflow(client, description, currentSender)
      logInfo("Registered coflow " + description.name + " with ID " + coflow.id)
      context.watch(sender)  // This doesn't work with remote actors but helps for testing
      sender ! RegisteredCoflow(coflow.id)

      // No need to schedule here. Changes won't happen until the new flows are added.
    }

    case UnregisterCoflow(coflowId) => {
      removeCoflow(idToCoflow.get(coflowId))
      sender ! true
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
      // Coflow termination is handled explicitly through UnregisterCoflow or when its client dies.
      actorToSlave.get(actor).foreach(removeSlave)
      actorToClient.get(actor).foreach(removeClient)
    }

    case RemoteClientDisconnected(transport, address) => {
      // The disconnected actor could've been a slave or a client; remove accordingly. 
      // Coflow termination is handled explicitly through UnregisterCoflow or when its client dies.
      addressToSlave.get(address).foreach(removeSlave)
      addressToClient.get(address).foreach(removeClient)
    }

    case RemoteClientShutdown(transport, address) => {
      // The disconnected actor could've been a slave or a client; remove accordingly. 
      // Coflow termination is handled explicitly through UnregisterCoflow or when its client dies.
      addressToSlave.get(address).foreach(removeSlave)
      addressToClient.get(address).foreach(removeClient)
    }

    case RequestMasterState => {
      sender ! MasterState(ip, port, idToSlave.values.toSeq.toArray, idToCoflow.values.toSeq.toArray, completedCoflows.toArray, 
        idToClient.values.toSeq.toArray, completedClients.toArray)
    }
    
    case RequestBestRxMachines(howMany, bytes) => {
      sender ! BestRxMachines(idToRxBps.getTopN(howMany, bytes).toArray.map(x => idToSlave.get(x).host))
    }
    
    case RequestBestTxMachines(howMany, bytes) => {
      sender ! BestTxMachines(idToTxBps.getTopN(howMany, bytes).toArray.map(x => idToSlave.get(x).host))
    }
    
    case AddFlow(flowDesc) => {
      // coflowId will always be valid
      val coflow = idToCoflow.get(flowDesc.coflowId)
      assert(coflow != null)
      
      logDebug("Adding flow to " + coflow)
      coflow.addFlow(flowDesc)
      sender ! true
      
      // No need to schedule here because a flow will not start until a receiver asks for it
    }
    
    case GetFlow(flowId, coflowId, clientId, slaveId, _) => {
      logDebug("Received GetFlow(" + flowId + ", " + coflowId + ", " + slaveId + ", " + sender + ")")
      val currentSender = sender
      Future { handleGetFlow(flowId, coflowId, clientId, slaveId, currentSender) }
    }
    
    case DeleteFlow(flowId, coflowId) => {
      // TODO: Actually do something; e.g., remove destination?
      // self ! ScheduleRequest
    }

    case ScheduleRequest => {
      schedule()
    }
  }

  def handleGetFlow(flowId: String, coflowId: String, clientId: String, slaveId: String, actor: ActorRef) {
    logDebug("handleGetFlow(" + flowId + ", " + coflowId + ", " + slaveId + ", " + sender + ")")
    
    // val slave = idToSlave(slaveId)
    // assert(slave != null)
    
    val client = idToClient.get(clientId)
    assert(client != null)

    val coflow = idToCoflow.get(coflowId)
    assert(coflow != null)
    // assert(coflow.contains(flowId))
    
    var canSchedule = false
    coflow.getFlowInfo(flowId) match {
      case Some(flowInfo) => {
        canSchedule = coflow.addDestination(flowId, client)
        logInfo("Added destination to " + coflow + ". " + (coflow.desc.maxFlows - coflow.getNumRegisteredFlows) + " flows remain.")

        // TODO: Always returning the default source. Considering selecting based on traffic etc.
        actor ! Some(GotFlowDesc(flowInfo.desc))
      }
      case None => {
        // logWarning("Couldn't find flow " + flowId + " of coflow " + coflowId)
        actor ! None
      }
    }

    if (canSchedule) {
      logInfo("Coflow " + coflowId + " ready to be scheduled")
      self ! ScheduleRequest
    }
  }

  def addSlave(id: String, host: String, port: Int, webUiPort: Int, commPort: Int,
    publicAddress: String, actor: ActorRef): SlaveInfo = {
    // There may be one or more refs to dead slaves on this same node (w/ diff. IDs), remove them.
    idToSlave.values.filter(w => (w.host == host) && (w.state == SlaveState.DEAD)).foreach(idToSlave.values.remove(_))
    val slave = new SlaveInfo(id, host, port, actor, webUiPort, commPort, publicAddress)
    idToSlave.put(slave.id, slave)
    actorToSlave(actor) = slave
    addressToSlave(actor.path.address) = slave
    hostToSlave(slave.host) = slave
    return slave
  }

  def removeSlave(slave: SlaveInfo) {
    logWarning("Removing slave " + slave.id + " on " + slave.host + ":" + slave.port)
    slave.setState(SlaveState.DEAD)
    // Do not remove from idToSlave so that we remember DEAD slaves
    actorToSlave -= slave.actor
    addressToSlave -= slave.actor.path.address
    hostToSlave -= slave.host
  }

  def addClient(clientName: String, host: String, commPort: Int, actor: ActorRef): ClientInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    val client = new ClientInfo(now, newClientId(date), host, commPort, date, actor)
    idToClient.put(client.id, client)
    actorToClient(actor) = client
    addressToClient(actor.path.address) = client
    return client
  }

  def removeClient(client: ClientInfo) {
    if (idToClient.containsValue(client)) {
      logInfo("Removing client " + client.id)
      idToClient.remove(client.id)
      actorToClient -= client.actor
      addressToClient -= client.actor.path.address
      completedClients += client  // Remember it in our history
      client.markFinished()
      
      client.coflows.foreach(removeCoflow)  // Remove child coflows as well
    }
  }

  def addCoflow(client:ClientInfo, desc: CoflowDescription, actor: ActorRef): CoflowInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    val coflow = new CoflowInfo(now, newCoflowId(date), desc, date, actor)
    
    idToCoflow.put(coflow.id, coflow)
    
    client.addCoflow(coflow)  // Update its parent client
    
    return coflow
  }

  // TODO: Let all involved clients know so that they can free up local resources
  def removeCoflow(coflow: CoflowInfo) {
    if (idToCoflow.containsValue(coflow)) {
      logInfo("Removing coflow " + coflow.id)
      idToCoflow.remove(coflow.id)
      completedCoflows += coflow  // Remember it in our history
      coflow.markFinished(CoflowState.FINISHED)  // TODO: Mark it as FAILED if it failed
      
      self ! ScheduleRequest
    }
  }

  /**
   * Schedule ongoing coflows and flows. 
   * Returns a Boolean indicating whether it ran or not
   */
  def schedule(): Boolean = synchronized {
    
    // If scheduled within last 100ms ignore this request
    val curTime = System.currentTimeMillis
    if (curTime - lastScheduled < SCHEDULE_FREQ) {
      return false
    }

    // Update when we last scheduled
    lastScheduled = curTime
    
    // STEP 1: Sort READY or RUNNING coflows by remaining size
    val sortedCoflows = idToCoflow.values.toBuffer.filter(x => x.state == CoflowState.READY || x.state == CoflowState.RUNNING)
    sortedCoflows.sortWith(_.remainingSizeInBytes < _.remainingSizeInBytes)
    
    // STEP 2: Perform WSS + Backfilling
    val sBpsFree = new HashMap[String, Double]().withDefaultValue(NIC_BPS)
    val rBpsFree = new HashMap[String, Double]().withDefaultValue(NIC_BPS)
    
    for (cf <- sortedCoflows) {
      logInfo("Scheduling " + cf)
      
      val sUsed = new HashMap[String, Double]().withDefaultValue(0.0)
      val rUsed = new HashMap[String, Double]().withDefaultValue(0.0)

      for (flowInfo <- cf.getFlows) {
        val src = flowInfo.source
        val dst = flowInfo.destClient.host

        val minFree = math.min(sBpsFree(src), rBpsFree(dst))
        if (minFree > 0.0) {
          flowInfo.currentBps = minFree * (flowInfo.getFlowSize() / cf.alpha)
          if (math.abs(flowInfo.currentBps) < 1e-6) 
            flowInfo.currentBps = 0.0
          flowInfo.lastScheduled = System.currentTimeMillis
          
          
          // Remember how much capacity was allocated
          sUsed(src) = sUsed(src) + flowInfo.currentBps
          rUsed(dst) = rUsed(dst) + flowInfo.currentBps
          
          // Set the coflow as running
          cf.state = CoflowState.RUNNING
        } else {
          flowInfo.currentBps = 0.0
        }
      }
      
      // Remove capacity from ALL sources and destination for this coflow
      for (sl <- idToSlave.values) {
        val host = sl.host
        sBpsFree(host) = sBpsFree(host) - sUsed(host)
        rBpsFree(host) = rBpsFree(host) - rUsed(host)
      }
    }
    
    logInfo("START_NEW_SCHEDULE")
    // STEP 3: Communicate updates to clients
    val activeFlows = sortedCoflows.filter(_.state == CoflowState.RUNNING).flatMap(_.getFlows)
    activeFlows.groupBy(_.destClient).foreach { tuple => 
      val client = tuple._1
      val flows = tuple._2
      
      val rateMap = flows.map(t => (t.desc, t.currentBps)).toMap
      
      // Log current schedule
      logDebug(client.host + " = " + rateMap.size + " flows")
      for ((fDesc, nBPS) <- rateMap) {
        logDebug(fDesc + " ==> " + nBPS + " bps")
      }
      
      client.actor ! UpdatedRates(rateMap)
    }
    logInfo("END_NEW_SCHEDULE")
    
    true
  }

  /** Generate a new coflow ID given a coflow's submission date */
  def newCoflowId(submitDate: Date): String = {
    // "coflow-%s-%04d".format(DATE_FORMAT.format(submitDate), nextCoflowNumber.getAndIncrement())
    "COFLOW-%06d".format(nextCoflowNumber.getAndIncrement())
  }

  /** Generate a new client ID given a client's connection date */
  def newClientId(submitDate: Date): String = {
    // "client-%s-%04d".format(DATE_FORMAT.format(submitDate), nextClientNumber.getAndIncrement())
    "CLIENT-%06d".format(nextClientNumber.getAndIncrement())
  }

  /** Check for, and remove, any timed-out slaves */
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
}

private[varys] object Master {
  private val systemName = "varysMaster"
  private val actorName = "Master"
  private val varysUrlRegex = "varys://([^:]+):([0-9]+)".r

  def main(argStrings: Array[String]) {
    val args = new MasterArguments(argStrings)
    val (actorSystem, _) = startSystemAndActor(args.ip, args.port, args.webUiPort)
    actorSystem.awaitTermination()
  }

  /** Returns an `akka://...` URL for the Master actor given a varysUrl `varys://host:ip`. */
  def toAkkaUrl(varysUrl: String): String = {
    varysUrl match {
      case varysUrlRegex(host, port) =>
        "akka://%s@%s:%s/user/%s".format(systemName, host, port, actorName)
      case _ =>
        throw new VarysException("Invalid master URL: " + varysUrl)
    }
  }

  def startSystemAndActor(host: String, port: Int, webUiPort: Int): (ActorSystem, Int) = {
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(systemName, host, port)
    val actor = actorSystem.actorOf(Props(new MasterActor(host, boundPort, webUiPort)), 
      name = actorName)
    (actorSystem, boundPort)
  }
}
