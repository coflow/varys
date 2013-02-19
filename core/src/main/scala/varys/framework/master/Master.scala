package varys.framework.master

import akka.actor._
import akka.actor.Terminated
import akka.remote.{RemoteClientLifeCycleEvent, RemoteClientDisconnected, RemoteClientShutdown}
import akka.util.duration._

import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import varys.framework._
import varys.{Logging, VarysException, Utils}
import varys.util.AkkaUtils


private[varys] class Master(ip: String, port: Int, webUiPort: Int) extends Actor with Logging {
  val DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss")  // For coflow IDs
  val SLAVE_TIMEOUT = System.getProperty("varys.slave.timeout", "60").toLong * 1000

  var nextCoflowNumber = 0
  val slaves = new HashSet[SlaveInfo]
  val idToSlave = new HashMap[String, SlaveInfo]
  val actorToSlave = new HashMap[ActorRef, SlaveInfo]
  val addressToSlave = new HashMap[Address, SlaveInfo]

  val idToRxBps = new SlaveToBpsMap
  val idToTxBps = new SlaveToBpsMap

  val coflows = new HashSet[CoflowInfo]
  val idToCoflow = new HashMap[String, CoflowInfo]
  val actorToCoflow = new HashMap[ActorRef, CoflowInfo]
  val addressToCoflow = new HashMap[Address, CoflowInfo]

  val waitingCoflows = new ArrayBuffer[CoflowInfo]
  val completedCoflows = new ArrayBuffer[CoflowInfo]

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
    case RegisterSlave(id, host, slavePort, cores, slave_webUiPort, publicAddress) => {
      logInfo("Registering slave %s:%d with %d cores".format(
        host, slavePort, cores))
      if (idToSlave.contains(id)) {
        sender ! RegisterSlaveFailed("Duplicate slave ID")
      } else {
        addSlave(id, host, slavePort, cores, slave_webUiPort, publicAddress)
        context.watch(sender)  // This doesn't work with remote actors but helps for testing
        sender ! RegisteredSlave("http://" + masterPublicAddress + ":" + webUiPort)
        // schedule()
      }
    }

    case RegisterCoflow(description) => {
      logInfo("Registering coflow " + description.name)
      val coflow = addCoflow(description, sender)
      logInfo("Registered coflow " + description.name + " with ID " + coflow.id)
      waitingCoflows += coflow
      context.watch(sender)  // This doesn't work with remote actors but helps for testing
      sender ! RegisteredCoflow(coflow.id)
      // schedule()
    }

    case Heartbeat(slaveId, newRxBps, newTxBps) => {
      idToSlave.get(slaveId) match {
        case Some(slaveInfo) =>
          slaveInfo.updateNetworkStats(newRxBps, newTxBps)
          slaveInfo.lastHeartbeat = System.currentTimeMillis()
          
          idToRxBps.updateNetworkStats(slaveId, newRxBps)
          idToTxBps.updateNetworkStats(slaveId, newTxBps)
          
        case None =>
          logWarning("Got heartbeat from unregistered slave " + slaveId)
      }
    }

    case Terminated(actor) => {
      // The disconnected actor could've been either a slave or a coflow; remove whichever of
      // those we have an entry for in the corresponding actor hashmap
      actorToSlave.get(actor).foreach(removeSlave)
      actorToCoflow.get(actor).foreach(removeCoflow)
    }

    case RemoteClientDisconnected(transport, address) => {
      // The disconnected client could've been either a slave or a coflow; remove whichever it was
      addressToSlave.get(address).foreach(removeSlave)
      addressToCoflow.get(address).foreach(removeCoflow)
    }

    case RemoteClientShutdown(transport, address) => {
      // The disconnected client could've been either a slave or a coflow; remove whichever it was
      addressToSlave.get(address).foreach(removeSlave)
      addressToCoflow.get(address).foreach(removeCoflow)
    }

    case RequestMasterState => {
      sender ! MasterState(ip, port, slaves.toArray, coflows.toArray, completedCoflows.toArray)
    }
  }

  def addSlave(id: String, host: String, port: Int, cores: Int, webUiPort: Int,
    publicAddress: String): SlaveInfo = {
    // There may be one or more refs to dead slaves on this same node (w/ different ID's), remove them.
    slaves.filter(w => (w.host == host) && (w.state == SlaveState.DEAD)).foreach(slaves -= _)
    val slave = new SlaveInfo(id, host, port, cores, sender, webUiPort, publicAddress)
    slaves += slave
    idToSlave(slave.id) = slave
    actorToSlave(sender) = slave
    addressToSlave(sender.path.address) = slave
    return slave
  }

  def removeSlave(slave: SlaveInfo) {
    logInfo("Removing slave " + slave.id + " on " + slave.host + ":" + slave.port)
    slave.setState(SlaveState.DEAD)
    idToSlave -= slave.id
    actorToSlave -= slave.actor
    addressToSlave -= slave.actor.path.address
  }

  def addCoflow(desc: CoflowDescription, driver: ActorRef): CoflowInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    val coflow = new CoflowInfo(now, newCoflowId(date), desc, date, driver)
    coflows += coflow
    idToCoflow(coflow.id) = coflow
    actorToCoflow(driver) = coflow
    addressToCoflow(driver.path.address) = coflow
    return coflow
  }

  def removeCoflow(coflow: CoflowInfo) {
    if (coflows.contains(coflow)) {
      logInfo("Removing coflow " + coflow.id)
      coflows -= coflow
      idToCoflow -= coflow.id
      actorToCoflow -= coflow.driver
      addressToSlave -= coflow.driver.path.address
      completedCoflows += coflow   // Remember it in our history
      waitingCoflows -= coflow
      coflow.markFinished(CoflowState.FINISHED)  // TODO: Mark it as FAILED if it failed
      // schedule()
    }
  }

  /** Generate a new coflow ID given a coflow's submission date */
  def newCoflowId(submitDate: Date): String = {
    val coflowId = "coflow-%s-%04d".format(DATE_FORMAT.format(submitDate), nextCoflowNumber)
    nextCoflowNumber += 1
    coflowId
  }

  /** Check for, and remove, any timed-out slaves */
  def timeOutDeadSlaves() {
    // Copy the slaves into an array so we don't modify the hashset while iterating through it
    val expirationTime = System.currentTimeMillis() - SLAVE_TIMEOUT
    val toRemove = slaves.filter(_.lastHeartbeat < expirationTime).toArray
    for (slave <- toRemove) {
      logWarning("Removing %s because we got no heartbeat in %d seconds".format(
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

  /** Returns an `akka://...` URL for the Master actor given a sparkUrl `spark://host:ip`. */
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
    val actor = actorSystem.actorOf(Props(new Master(host, boundPort, webUiPort)), name = actorName)
    (actorSystem, boundPort)
  }
}
