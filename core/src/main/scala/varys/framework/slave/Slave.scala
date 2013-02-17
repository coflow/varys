package varys.framework.slave

import scala.collection.mutable.{ArrayBuffer, HashMap}
import akka.actor.{ActorRef, Props, Actor, ActorSystem, Terminated}
import akka.util.duration._
import varys.{Logging, Utils}
import varys.util.AkkaUtils
import varys.framework._
import akka.remote.{RemoteClientLifeCycleEvent, RemoteClientShutdown, RemoteClientDisconnected}
import java.text.SimpleDateFormat
import java.util.Date
import varys.framework.RegisterSlave
import varys.framework.RegisterSlaveFailed
import varys.framework.master.Master
import java.io.File

private[varys] class Slave(
    ip: String,
    port: Int,
    webUiPort: Int,
    cores: Int,
    memory: Int,
    masterUrl: String,
    workDirPath: String = null)
  extends Actor with Logging {

  val DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss")  // For slave IDs

  // Send a heartbeat every (heartbeat timeout) / 4 milliseconds
  val HEARTBEAT_MILLIS = System.getProperty("varys.slave.timeout", "60").toLong * 1000 / 4

  var master: ActorRef = null
  var masterWebUiUrl : String = ""
  val slaveId = generateSlaveId()
  var varysHome: File = null
  var workDir: File = null
  val publicAddress = {
    val envVar = System.getenv("VARYS_PUBLIC_DNS")
    if (envVar != null) envVar else ip
  }

  var coresUsed = 0
  var memoryUsed = 0

  def coresFree: Int = cores - coresUsed
  def memoryFree: Int = memory - memoryUsed

  def createWorkDir() {
    workDir = Option(workDirPath).map(new File(_)).getOrElse(new File(varysHome, "work"))
    try {
      if (!workDir.exists() && !workDir.mkdirs()) {
        logError("Failed to create work directory " + workDir)
        System.exit(1)
      }
    } catch {
      case e: Exception =>
        logError("Failed to create work directory " + workDir, e)
        System.exit(1)
    }
  }

  override def preStart() {
    logInfo("Starting Varys slave %s:%d with %d cores, %s RAM".format(
      ip, port, cores, Utils.memoryMegabytesToString(memory)))
    varysHome = new File(Option(System.getenv("VARYS_HOME")).getOrElse("."))
    logInfo("Varys home: " + varysHome)
    createWorkDir()
    connectToMaster()
    startWebUi()
  }

  def connectToMaster() {
    logInfo("Connecting to master " + masterUrl)
    try {
      master = context.actorFor(Master.toAkkaUrl(masterUrl))
      master ! RegisterSlave(slaveId, ip, port, cores, memory, webUiPort, publicAddress)
      context.system.eventStream.subscribe(self, classOf[RemoteClientLifeCycleEvent])
      context.watch(master) // Doesn't work with remote actors, but useful for testing
    } catch {
      case e: Exception =>
        logError("Failed to connect to master", e)
        System.exit(1)
    }
  }

  def startWebUi() {
    val webUi = new SlaveWebUI(context.system, self)
    try {
      AkkaUtils.startSprayServer(context.system, "0.0.0.0", webUiPort, webUi.handler)
    } catch {
      case e: Exception =>
        logError("Failed to create web UI", e)
        System.exit(1)
    }
  }

  override def receive = {
    case RegisteredSlave(url) =>
      masterWebUiUrl = url
      logInfo("Successfully registered with master")
      context.system.scheduler.schedule(0 millis, HEARTBEAT_MILLIS millis) {
        master ! Heartbeat(slaveId)
      }

    case RegisterSlaveFailed(message) =>
      logError("Slave registration failed: " + message)
      System.exit(1)

    case Terminated(_) | RemoteClientDisconnected(_, _) | RemoteClientShutdown(_, _) =>
      masterDisconnected()
      
    case RequestSlaveState => {
      sender ! SlaveState(ip, port, slaveId, masterUrl, cores, memory, 
        coresUsed, memoryUsed, masterWebUiUrl)
    }
  }

  def masterDisconnected() {
    // TODO: It would be nice to try to reconnect to the master, but just shut down for now.
    // (Note that if reconnecting we would also need to assign IDs differently.)
    logError("Connection to master failed! Shutting down.")
    System.exit(1)
  }

  def generateSlaveId(): String = {
    "slave-%s-%s-%d".format(DATE_FORMAT.format(new Date), ip, port)
  }
}

private[varys] object Slave {
  def main(argStrings: Array[String]) {
    val args = new SlaveArguments(argStrings)
    val (actorSystem, _) = startSystemAndActor(args.ip, args.port, args.webUiPort, args.cores,
      args.memory, args.master, args.workDir)
    actorSystem.awaitTermination()
  }

  def startSystemAndActor(host: String, port: Int, webUiPort: Int, cores: Int, memory: Int,
    masterUrl: String, workDir: String, slaveNumber: Option[Int] = None): (ActorSystem, Int) = {
    // The LocalVarysCluster runs multiple local varysSlaveX actor systems
    val systemName = "varysSlave" + slaveNumber.map(_.toString).getOrElse("")
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(systemName, host, port)
    val actor = actorSystem.actorOf(Props(new Slave(host, boundPort, webUiPort, cores, memory,
      masterUrl, workDir)), name = "Slave")
    (actorSystem, boundPort)
  }

}
