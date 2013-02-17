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
  val DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss")  // For job IDs
  val SLAVE_TIMEOUT = System.getProperty("varys.slave.timeout", "60").toLong * 1000

  var nextJobNumber = 0
  val slaves = new HashSet[SlaveInfo]
  val idToSlave = new HashMap[String, SlaveInfo]
  val actorToSlave = new HashMap[ActorRef, SlaveInfo]
  val addressToSlave = new HashMap[Address, SlaveInfo]

  val jobs = new HashSet[JobInfo]
  val idToJob = new HashMap[String, JobInfo]
  val actorToJob = new HashMap[ActorRef, JobInfo]
  val addressToJob = new HashMap[Address, JobInfo]

  val waitingJobs = new ArrayBuffer[JobInfo]
  val completedJobs = new ArrayBuffer[JobInfo]

  val masterPublicAddress = {
    val envVar = System.getenv("VARYS_PUBLIC_DNS")
    if (envVar != null) envVar else ip
  }

  // As a temporary workaround before better ways of configuring memory, we allow users to set
  // a flag that will perform round-robin scheduling across the nodes (spreading out each job
  // among all the nodes) instead of trying to consolidate each job onto a small # of nodes.
  val spreadOutJobs = System.getProperty("varys.framework.spreadOut", "false").toBoolean

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
    case RegisterSlave(id, host, slavePort, cores, memory, slave_webUiPort, publicAddress) => {
      logInfo("Registering slave %s:%d with %d cores, %s RAM".format(
        host, slavePort, cores, Utils.memoryMegabytesToString(memory)))
      if (idToSlave.contains(id)) {
        sender ! RegisterSlaveFailed("Duplicate slave ID")
      } else {
        addSlave(id, host, slavePort, cores, memory, slave_webUiPort, publicAddress)
        context.watch(sender)  // This doesn't work with remote actors but helps for testing
        sender ! RegisteredSlave("http://" + masterPublicAddress + ":" + webUiPort)
        // schedule()
      }
    }

    case RegisterJob(description) => {
      logInfo("Registering job " + description.name)
      val job = addJob(description, sender)
      logInfo("Registered job " + description.name + " with ID " + job.id)
      waitingJobs += job
      context.watch(sender)  // This doesn't work with remote actors but helps for testing
      sender ! RegisteredJob(job.id)
      // schedule()
    }

    case Heartbeat(slaveId) => {
      idToSlave.get(slaveId) match {
        case Some(slaveInfo) =>
          slaveInfo.lastHeartbeat = System.currentTimeMillis()
        case None =>
          logWarning("Got heartbeat from unregistered slave " + slaveId)
      }
    }

    case Terminated(actor) => {
      // The disconnected actor could've been either a slave or a job; remove whichever of
      // those we have an entry for in the corresponding actor hashmap
      actorToSlave.get(actor).foreach(removeSlave)
      actorToJob.get(actor).foreach(removeJob)
    }

    case RemoteClientDisconnected(transport, address) => {
      // The disconnected client could've been either a slave or a job; remove whichever it was
      addressToSlave.get(address).foreach(removeSlave)
      addressToJob.get(address).foreach(removeJob)
    }

    case RemoteClientShutdown(transport, address) => {
      // The disconnected client could've been either a slave or a job; remove whichever it was
      addressToSlave.get(address).foreach(removeSlave)
      addressToJob.get(address).foreach(removeJob)
    }

    case RequestMasterState => {
      sender ! MasterState(ip, port, slaves.toArray, jobs.toArray, completedJobs.toArray)
    }
  }

  /**
   * Can a job use the given slave? True if the slave has enough memory.
   */
  def canUse(job: JobInfo, slave: SlaveInfo): Boolean = {
    slave.memoryFree >= job.desc.memoryPerSlave
  }

  def addSlave(id: String, host: String, port: Int, cores: Int, memory: Int, webUiPort: Int,
    publicAddress: String): SlaveInfo = {
    // There may be one or more refs to dead slaves on this same node (w/ different ID's), remove them.
    slaves.filter(w => (w.host == host) && (w.state == SlaveState.DEAD)).foreach(slaves -= _)
    val slave = new SlaveInfo(id, host, port, cores, memory, sender, webUiPort, publicAddress)
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

  def addJob(desc: JobDescription, driver: ActorRef): JobInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    val job = new JobInfo(now, newJobId(date), desc, date, driver)
    jobs += job
    idToJob(job.id) = job
    actorToJob(driver) = job
    addressToJob(driver.path.address) = job
    return job
  }

  def removeJob(job: JobInfo) {
    if (jobs.contains(job)) {
      logInfo("Removing job " + job.id)
      jobs -= job
      idToJob -= job.id
      actorToJob -= job.driver
      addressToSlave -= job.driver.path.address
      completedJobs += job   // Remember it in our history
      waitingJobs -= job
      job.markFinished(JobState.FINISHED)  // TODO: Mark it as FAILED if it failed
      // schedule()
    }
  }

  /** Generate a new job ID given a job's submission date */
  def newJobId(submitDate: Date): String = {
    val jobId = "job-%s-%04d".format(DATE_FORMAT.format(submitDate), nextJobNumber)
    nextJobNumber += 1
    jobId
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

  /** Returns an /bin/bash: akka://...: No such file or directory URL for the Master actor given a varysUrl /bin/bash: varys://host:ip: No such file or directory. */
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
