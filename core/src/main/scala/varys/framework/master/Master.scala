package varys.framework.master

import akka.actor._
import akka.actor.Terminated
import akka.remote.{RemoteClientLifeCycleEvent, RemoteClientDisconnected, RemoteClientShutdown}
import akka.util.duration._
import akka.dispatch._
import akka.routing._

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.atomic._
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.collection.JavaConversions._

import varys.framework._
import varys.{Logging, VarysException, Utils}
import varys.util.{AkkaUtils, SlaveToBpsMap}

private[varys] class Master(
    systemName:String, 
    actorName: String, 
    host: String, 
    port: Int, 
    webUiPort: Int) 
  extends Logging {
  
  val NUM_MASTER_INSTANCES = System.getProperty("varys.master.numInstances", "10").toInt
  val DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss")  // For coflow IDs
  val SLAVE_TIMEOUT = System.getProperty("varys.slave.timeout", "60").toLong * 1000
  val NIC_BitPS = 1024 * 1048576
  
  val CONSIDER_DEADLINE = System.getProperty("varys.master.consdierDeadline", "false").toBoolean
  val DEADLINE_PADDING = System.getProperty("varys.master.deadlinePadding", "0.1").toDouble

  val idToSlave = new ConcurrentHashMap[String, SlaveInfo]()
  val actorToSlave = new ConcurrentHashMap[ActorRef, SlaveInfo]
  val addressToSlave = new ConcurrentHashMap[Address, SlaveInfo]
  val hostToSlave = new ConcurrentHashMap[String, SlaveInfo]

  val idToRxBps = new SlaveToBpsMap
  val idToTxBps = new SlaveToBpsMap

  var nextCoflowNumber = new AtomicInteger()
  val idToCoflow = new ConcurrentHashMap[String, CoflowInfo]()
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
    val actor = actorSystem.actorOf(Props(new MasterActor(host, boundPort, webUiPort)).withRouter(
      RoundRobinRouter(nrOfInstances = NUM_MASTER_INSTANCES)), 
      name = actorName)
    (actorSystem, boundPort)
  }
  
  private[varys] class MasterActor(
      ip: String, 
      port: Int, 
      webUiPort: Int) 
    extends Actor with Logging {

    val masterPublicAddress = {
      val envVar = System.getenv("VARYS_PUBLIC_DNS")
      if (envVar != null) envVar else ip
    }

    override def preStart() {
      logInfo("Starting Varys master at varys://" + ip + ":" + port)
      // Listen for remote client disconnection events, since they don't go through Akka's watch()
      context.system.eventStream.subscribe(self, classOf[RemoteClientLifeCycleEvent])
      if (!webUiStarted.getAndSet(true))
        startWebUi()
      // context.system.scheduler.schedule(0 millis, SLAVE_TIMEOUT millis)(timeOutDeadSlaves())
    }

    def startWebUi() {
      try {
        val webUi = new MasterWebUI(context.system, self)
        AkkaUtils.startSprayServer(context.system, "0.0.0.0", webUiPort, webUi.handler)
      } catch {
        case e: Exception =>
          logError("Failed to create web UI", e)
          System.exit(1)
      }
    }

    override def receive = {
      case RegisterSlave(id, host, slavePort, slave_webUiPort, slave_commPort, publicAddress) => {
        val currentSender = sender
        logInfo("Registering slave %s:%d".format(host, slavePort))
        if (idToSlave.containsKey(id)) {
          currentSender ! RegisterSlaveFailed("Duplicate slave ID")
        } else {
          addSlave(id, host, slavePort, slave_webUiPort, slave_commPort, publicAddress, currentSender)
          // context.watch(currentSender)  // This doesn't work with remote actors but helps for testing
          currentSender ! RegisteredSlave("http://" + masterPublicAddress + ":" + webUiPort)
        }
      }

      case RegisterClient(clientName, host, commPort) => {
        val currentSender = sender
        val st = now
        logTrace("Registering client %s@%s:%d".format(clientName, host, commPort))
        if (hostToSlave.containsKey(host)) {
          val client = addClient(clientName, host, commPort, currentSender)
          // context.watch(currentSender)  // This doesn't work with remote actors but helps for testing
          val slave = hostToSlave(host)
          currentSender ! RegisteredClient(client.id, slave.id, "varys://" + slave.host + ":" + slave.port)
          logInfo("Registered client " + clientName + " with ID " + client.id + " in " + (now - st) + " milliseconds")
        } else {
          currentSender ! RegisterClientFailed("No Varys slave at " + host)
        }
      }

      case RegisterCoflow(clientId, description) => {
        val currentSender = sender
        val st = now
        logTrace("Registering coflow " + description.name)

        if (CONSIDER_DEADLINE && description.deadlineMillis == 0) {
          currentSender ! RegisterCoflowFailed("Must specify a valid deadline")
        } else {
          val client = idToClient.get(clientId)
          if (client == null) {
            currentSender ! RegisterCoflowFailed("Invalid clientId " + clientId)
          } else {
            val coflow = addCoflow(client, description, currentSender)
            // context.watch(currentSender)  // This doesn't work with remote actors but helps for testing
            currentSender ! RegisteredCoflow(coflow.id)
            logInfo("Registered coflow " + description.name + " with ID " + coflow.id + " in " + (now - st) + " milliseconds")
            // No need to schedule here. Changes won't happen until the new flows are added.
          }
        }
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
        if (actorToSlave.containsKey(actor))
          removeSlave(actorToSlave.get(actor))
        if (actorToClient.containsKey(actor))  
          removeClient(actorToClient.get(actor))
      }

      case RemoteClientDisconnected(transport, address) => {
        // The disconnected actor could've been a slave or a client; remove accordingly. 
        // Coflow termination is handled explicitly through UnregisterCoflow or when its client dies.
        if (addressToSlave.containsKey(address))
          removeSlave(addressToSlave.get(address))
        if (addressToClient.containsKey(address))  
          removeClient(addressToClient.get(address))
      }

      case RemoteClientShutdown(transport, address) => {
        // The disconnected actor could've been a slave or a client; remove accordingly. 
        // Coflow termination is handled explicitly through UnregisterCoflow or when its client dies.
        if (addressToSlave.containsKey(address))
          removeSlave(addressToSlave.get(address))
        if (addressToClient.containsKey(address))  
          removeClient(addressToClient.get(address))
      }

      case RequestMasterState => {
        sender ! MasterState(ip, port, idToSlave.values.toSeq.toArray, idToCoflow.values.toSeq.toArray, completedCoflows.toArray, 
          idToClient.values.toSeq.toArray)
      }

      case RequestBestRxMachines(howMany, bytes) => {
        sender ! BestRxMachines(idToRxBps.getTopN(howMany, bytes).toArray.map(x => idToSlave.get(x).host))
      }

      case RequestBestTxMachines(howMany, bytes) => {
        sender ! BestTxMachines(idToTxBps.getTopN(howMany, bytes).toArray.map(x => idToSlave.get(x).host))
      }

      case AddFlows(flowDescs, coflowId, dataType) => {
        val currentSender = sender

        // coflowId will always be valid
        val coflow = idToCoflow.get(coflowId)
        assert(coflow != null)

        val st = now
        flowDescs.foreach { coflow.addFlow }
        logDebug("Added " + flowDescs.size + " flows to " + coflow + " in " + (now - st) + " milliseconds")
        currentSender ! true

        // No need to schedule here because a flow will not start until a receiver asks for it
      }

      case AddFlow(flowDesc) => {
        val currentSender = sender

        // coflowId will always be valid
        val coflow = idToCoflow.get(flowDesc.coflowId)
        assert(coflow != null)

        val st = now
        coflow.addFlow(flowDesc)
        logDebug("Added flow to " + coflow + " in " + (now - st) + " milliseconds")
        currentSender ! true

        // No need to schedule here because a flow will not start until a receiver asks for it
      }

      case GetFlow(flowId, coflowId, clientId, slaveId, _) => {
        logTrace("Received GetFlow(" + flowId + ", " + coflowId + ", " + slaveId + ", " + sender + ")")
        val currentSender = sender
        Future { handleGetFlow(flowId, coflowId, clientId, slaveId, currentSender) }
      }

      case GetFlows(flowIds, coflowId, clientId, slaveId, _) => {
        logTrace("Received GetFlows(" + flowIds + ", " + coflowId + ", " + slaveId + ", " + sender + ")")
        val currentSender = sender
        Future { handleGetFlows(flowIds, coflowId, clientId, slaveId, currentSender) }
      }

      case FlowProgress(flowId, coflowId, bytesSinceLastUpdate, isCompleted) => {
        // coflowId will always be valid
        val coflow = idToCoflow.get(coflowId)
        assert(coflow != null)

        val st = now
        coflow.updateFlow(flowId, bytesSinceLastUpdate, isCompleted)
        logTrace("Received FlowProgress for flow " + flowId + " of " + coflow + " in " + (now - st) + " milliseconds")
      }

      case DeleteFlow(flowId, coflowId) => {
        // TODO: Actually do something; e.g., remove destination?
        // self ! ScheduleRequest
        // sender ! true
      }

      case ScheduleRequest => {
        schedule()
      }
    }

    def handleGetFlows(flowIds: Array[String], coflowId: String, clientId: String, slaveId: String, actor: ActorRef) {
      logTrace("handleGetFlows(" + flowIds + ", " + coflowId + ", " + slaveId + ", " + actor + ")")

      val client = idToClient.get(clientId)
      assert(client != null)

      val coflow = idToCoflow.get(coflowId)
      assert(coflow != null)
      // assert(coflow.contains(flowId))

      var canSchedule = false
      coflow.getFlowInfos(flowIds) match {
        case Some(flowInfos) => {
          val st = now
          canSchedule = coflow.addDestinations(flowIds, client)

          // TODO: Always returning the default source. Consider selecting based on traffic etc.
          actor ! Some(GotFlowDescs(flowInfos.map(_.desc)))

          logInfo("Added " + flowIds.size + " destinations to " + coflow + ". " + coflow.numFlowsToRegister + " flows remain to register; in " + (now - st) + " milliseconds")
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

    def handleGetFlow(flowId: String, coflowId: String, clientId: String, slaveId: String, actor: ActorRef) {
      logTrace("handleGetFlow(" + flowId + ", " + coflowId + ", " + slaveId + ", " + actor + ")")

      val client = idToClient.get(clientId)
      assert(client != null)

      val coflow = idToCoflow.get(coflowId)
      assert(coflow != null)
      // assert(coflow.contains(flowId))

      var canSchedule = false
      coflow.getFlowInfo(flowId) match {
        case Some(flowInfo) => {
          val st = now
          canSchedule = coflow.addDestination(flowId, client)

          // TODO: Always returning the default source. Considering selecting based on traffic etc.
          actor ! Some(GotFlowDesc(flowInfo.desc))

          logInfo("Added destination to " + coflow + ". " + coflow.numFlowsToRegister + " flows remain to register; in " + (now - st) + " milliseconds")
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
      slave.setState(SlaveState.DEAD)
      logError("Removing " + slave)
      // Do not remove from idToSlave so that we remember DEAD slaves
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
      return client
    }

    def removeClient(client: ClientInfo) {
      if (client != null && idToClient.containsValue(client)) {
        logTrace("Removing " + client)
        idToClient.remove(client.id)
        actorToClient -= client.actor
        addressToClient -= client.actor.path.address
        client.markFinished()

        client.coflows.foreach(removeCoflow)  // Remove child coflows as well
      }
    }

    def addCoflow(client: ClientInfo, desc: CoflowDescription, actor: ActorRef): CoflowInfo = {
      val now = System.currentTimeMillis()
      val date = new Date(now)
      val coflow = new CoflowInfo(now, newCoflowId(date), desc, client, date, actor)

      idToCoflow.put(coflow.id, coflow)

      client.addCoflow(coflow)  // Update its parent client

      return coflow
    }

    // TODO: Let all involved clients know so that they can free up local resources
    def removeCoflow(coflow: CoflowInfo) {
      removeCoflow(coflow, CoflowState.FINISHED, true)
    }

    def removeCoflow(coflow: CoflowInfo, endState: CoflowState.Value, reschedule: Boolean) {
      if (coflow != null && idToCoflow.containsValue(coflow)) {
        idToCoflow.remove(coflow.id)
        completedCoflows += coflow  // Remember it in our history
        coflow.markFinished(endState)
        logInfo("Removing " + coflow)

        if (reschedule) {
          self ! ScheduleRequest
        }
      }
    }

    /**
     * Schedule ongoing coflows and flows. 
     * Returns a Boolean indicating whether it ran or not
     */
    def schedule(): Boolean = synchronized {
      var st = now
      val markedForRejection = new ArrayBuffer[CoflowInfo]()

      // STEP 1: Sort READY or RUNNING coflows by remaining bottleneck size
      var sortedCoflows = idToCoflow.values.toBuffer.filter(x => x.remainingSizeInBytes > 0 && (x.curState == CoflowState.READY || x.curState == CoflowState.RUNNING))
      if (CONSIDER_DEADLINE) {
        sortedCoflows = sortedCoflows.sortWith(_.readyTime < _.readyTime)
      } else {
        sortedCoflows = sortedCoflows.sortWith(_.calcAlpha < _.calcAlpha)
      }
      val step1Dur = now - st
      st = now

      // STEP 2: Perform WSS + Backfilling
      val sBpsFree = new HashMap[String, Double]().withDefaultValue(NIC_BitPS)
      val rBpsFree = new HashMap[String, Double]().withDefaultValue(NIC_BitPS)

      for (cf <- sortedCoflows) {
        // FIXME: Using 200 milliseconds, i.e., 25MB size, as threshold
        val minMillis = math.max(cf.calcRemainingMillis(sBpsFree, rBpsFree) * (1 + DEADLINE_PADDING), 200)

        logInfo("Scheduling " + cf + " minMillis=" + minMillis)

        if (CONSIDER_DEADLINE 
            && cf.curState == CoflowState.READY
            && minMillis > cf.desc.deadlineMillis) {

          // Mark coflow for rejection, which will be removed later
          markedForRejection += cf

          val rejectMessage = "Minimum completion time of " + minMillis + " millis is more than the deadline of " + cf.desc.deadlineMillis + " millis"
          logInfo("Marking " + cf + " for rejection => " + rejectMessage)
        } else {
          val sUsed = new HashMap[String, Double]().withDefaultValue(0.0)
          val rUsed = new HashMap[String, Double]().withDefaultValue(0.0)

          for (flowInfo <- cf.getFlows) {
            val src = flowInfo.source
            val dst = flowInfo.destClient.host

            val minFree = math.min(sBpsFree(src), rBpsFree(dst))
            if (minFree > 0.0) {
              if (CONSIDER_DEADLINE) {
                flowInfo.currentBps = math.min((flowInfo.bytesLeft.toDouble * 8) / (cf.desc.deadlineMillis.toDouble / 1000), minFree)
              } else {
                flowInfo.currentBps = minFree * (flowInfo.getFlowSize() / cf.origAlpha)
              }
              if (math.abs(flowInfo.currentBps) < 1e-6) 
                flowInfo.currentBps = 0.0
              flowInfo.lastScheduled = System.currentTimeMillis

              // Remember how much capacity was allocated
              sUsed(src) = sUsed(src) + flowInfo.currentBps
              rUsed(dst) = rUsed(dst) + flowInfo.currentBps

              // Set the coflow as running
              cf.changeState(CoflowState.RUNNING)
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
      }

      // Keep only the RUNNING coflows
      sortedCoflows = sortedCoflows.filter(_.curState == CoflowState.RUNNING)

      // STEP2A: Work conservation
      for (cf <- sortedCoflows) {
        var totalBps = 0.0
        for (flowInfo <- cf.getFlows) {
          val src = flowInfo.source
          val dst = flowInfo.destClient.host

          val minFree = math.min(sBpsFree(src), rBpsFree(dst))
          if (minFree > 0.0) {
            flowInfo.currentBps += minFree
            sBpsFree(src) = sBpsFree(src) - minFree
            rBpsFree(dst) = rBpsFree(dst) - minFree
          }
          
          totalBps += flowInfo.currentBps
        }
        
        // Update current allocation of the coflow
        cf.setCurrentAllocation(totalBps)
      }
      val step2Dur = now - st
      st = now

      // STEP 3: Communicate updates to clients
      val activeFlows = sortedCoflows.flatMap(_.getFlows)
      logInfo("START_NEW_SCHEDULE: " + activeFlows.size + " flows in " + sortedCoflows.size + " coflows")
      
      for (cf <- sortedCoflows) {
        val (timeStamp, totalBps) = cf.currentAllocation
        logInfo(cf + " ==> " + (totalBps / 1048576.0) + " Mbps @ " + timeStamp)
      }
      
      activeFlows.groupBy(_.destClient).foreach { tuple => 
        val client = tuple._1
        val flows = tuple._2
        val rateMap = flows.map(t => (t.desc.dataId, t.currentBps)).toMap
        client.actor ! UpdatedRates(rateMap)
      }
      val step3Dur = now - st
      logInfo("END_NEW_SCHEDULE in " + (step1Dur + step2Dur + step3Dur) + " = (" + step1Dur + "+" + step2Dur + "+" + step3Dur + ") milliseconds")

      // STEP 4: Remove rejected coflows
      for (cf <- markedForRejection) {
        val rejectMessage = "Cannot meet the specified deadline of " + cf.desc.deadlineMillis + " milliseconds"
        
        cf.parentClient.actor ! RejectedCoflow(cf.id, rejectMessage)
        cf.getFlows.groupBy(_.destClient).foreach { tuple => 
          val client = tuple._1
          client.actor ! RejectedCoflow(cf.id, rejectMessage)
        }

        removeCoflow(cf, CoflowState.REJECTED, false)
      }

      true
    }

    /** 
     * Generate a new coflow ID given a coflow's submission date 
     */
    def newCoflowId(submitDate: Date): String = {
      // "coflow-%s-%04d".format(DATE_FORMAT.format(submitDate), nextCoflowNumber.getAndIncrement())
      "COFLOW-%06d".format(nextCoflowNumber.getAndIncrement())
    }

    /** 
     * Generate a new client ID given a client's connection date 
     */
    def newClientId(submitDate: Date): String = {
      // "client-%s-%04d".format(DATE_FORMAT.format(submitDate), nextClientNumber.getAndIncrement())
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

}
