package varys.framework.slave

import java.io.{File, ObjectInputStream, ObjectOutputStream, IOException}
import java.text.SimpleDateFormat
import java.util.Date
import java.net._

import scala.collection.mutable.{ArrayBuffer, HashMap}

import akka.actor.{ActorRef, Address, Props, Actor, ActorSystem, Terminated}
import akka.util.duration._
import akka.remote.{RemoteClientLifeCycleEvent, RemoteClientShutdown, RemoteClientDisconnected}

import varys.framework.master.Master
import varys.{VarysCommon, Logging, Utils, VarysException}
import varys.util.AkkaUtils
import varys.framework._

import org.hyperic.sigar.{Sigar, SigarException, NetInterfaceStat}

import com.google.common.io.Files

private[varys] class SlaveActor(
    ip: String,
    port: Int,
    webUiPort: Int,
    commPort: Int,
    masterUrl: String,
    workDirPath: String = null)
  extends Actor with Logging {
  
  var stopServer = false
  val serverThreadName = "ServerThread for Slave@" + Utils.localHostName()
  val serverThread = new Thread(serverThreadName) {
    override def run() {
      var threadPool = Utils.newDaemonCachedThreadPool
      var serverSocket: ServerSocket = new ServerSocket(commPort)

      try {
        while (!stopServer) {
          var clientSocket: Socket = null
          try {
            serverSocket.setSoTimeout(VarysCommon.HEARTBEAT_SEC * 1000)
            clientSocket = serverSocket.accept
          } catch {
            case e: Exception => { 
              if (stopServer) {
                logInfo("Stopping " + serverThreadName)
              }
            }
          }

          if (clientSocket != null) {
            try {
              threadPool.execute (new Thread {
                override def run: Unit = {
                  val oos = new ObjectOutputStream(clientSocket.getOutputStream)
                  oos.flush
                  val ois = new ObjectInputStream(clientSocket.getInputStream)
              
                  try {
                    val req = ois.readObject.asInstanceOf[GetRequest]
                    // TODO: Should it be any type?
                    val toSend: Option[Array[Byte]] = req.flowDesc.flowType match {
                      case FlowType.FAKE => {
                        // Create Data
                        val size = req.flowDesc.sizeInBytes.toInt
                        Some(Array.tabulate[Byte](size)(_.toByte))
                      }

                      case FlowType.ONDISK => {
                        // Read data from file into memory and send it
                        val fileDesc = req.flowDesc.asInstanceOf[FileDescription]
                        Some(Files.toByteArray(new File(fileDesc.pathToFile)))
                      }

                      case FlowType.INMEMORY => {
                        logWarning("FlowType.INMEMORY shouldn't have reached a slave!")
                        None
                      }

                      case _ => {
                        logWarning("Invalid FlowType!")
                        None
                      }
                    }
                
                    oos.writeObject(toSend)
                    oos.flush
                  } catch {
                    case e: Exception => {
                      logInfo (serverThreadName + " had a " + e)
                    }
                  } finally {
                    ois.close
                    oos.close
                    clientSocket.close
                  }
                }
              })
            } catch {
              // In failure, close socket here; else, client thread will close
              case ioe: IOException => {
                clientSocket.close
              }
            }
          }
        }
      } finally {
        serverSocket.close
      }
      // Shutdown the thread pool
      threadPool.shutdown
    }
  }
  serverThread.setDaemon(true)
  serverThread.start()

  // TODO: Keep track of local data
  val idsToFlow = new HashMap[(String, String), FlowDescription]

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

  var sigar = new Sigar()
  var lastRxBytes = -1.0
  var lastTxBytes = -1.0
  
  var curRxBps = 0.0
  var curTxBps = 0.0

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
    logInfo("Starting Varys slave %s:%d".format(ip, port))
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
      masterAddress = master.path.address
      master ! RegisterSlave(slaveId, ip, port, webUiPort, commPort, publicAddress)
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
    case RegisteredSlave(url) => {
      masterWebUiUrl = url
      logInfo("Successfully registered with master")

      // Thread to periodically uodate last{Rx|Tx}Bytes
      context.system.scheduler.schedule(0 millis, VarysCommon.HEARTBEAT_SEC * 1000 millis) {
        updateNetStats()
        master ! Heartbeat(slaveId, curRxBps, curTxBps)
      }
    }

    case RegisterSlaveFailed(message) => {
      logError("Slave registration failed: " + message)
      System.exit(1)
    }

    case Terminated(actor) if actor == master => {
      masterDisconnected()
    }
    
    case RemoteClientDisconnected(_, address) if address == masterAddress => {
      masterDisconnected()
    }
     
    case RemoteClientShutdown(_, address) if address == masterAddress => {
      masterDisconnected()
    }

    case RequestSlaveState => {
      sender ! SlaveState(ip, port, slaveId, masterUrl, curRxBps, curTxBps, masterWebUiUrl)
    }
    
    case RegisteredCoflow(coflowId) => {
      // TODO: Do something!
      sender ! true
    }
    
    case UnregisterCoflow(coflowId) => {
      // TODO: Do something!
      sender ! true
    }
    
    case AddFlow(flowDesc) => {
      // TODO: Do something!
      logInfo("Handling " + flowDesc)
      
      // Update commPort if the end point will be a client
      if (flowDesc.flowType != FlowType.INMEMORY) {
        flowDesc.updateCommPort(commPort)
      }
      
      // Now let the master know and notify the client
      AkkaUtils.tellActor(master, AddFlow(flowDesc))
      sender ! true
    }
    
    case GetFlow(flowId, coflowId, clientId, _, flowDesc) => {
      // TODO: Do something!
      
      sender ! true
    }
    
    case DeleteFlow(flowId, coflowId) => {
      // TODO: Actually remove
      sender ! true
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
      rxBps = (curRxBytes - lastRxBytes) / VarysCommon.HEARTBEAT_SEC;
      txBps = (curTxBytes - lastTxBytes) / VarysCommon.HEARTBEAT_SEC;
    } 
    
    lastRxBytes = curRxBytes
    lastTxBytes = curTxBytes
    
    curRxBps = rxBps
    curTxBps = txBps

    // FIXME: Sometimes Sigar stops responding, and printing something in this method brings it back
    // logInfo(rxBps + " " + txBps)
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

  /** Returns an `akka://...` URL for the Master actor given a varysUrl `varys://host:ip`. */
  def toAkkaUrl(varysUrl: String): String = {
    varysUrl match {
      case varysUrlRegex(host, port) =>
        "akka://%s@%s:%s/user/%s".format(systemName, host, port, actorName)
      case _ =>
        throw new VarysException("Invalid master URL: " + varysUrl)
    }
  }

  def startSystemAndActor(host: String, port: Int, webUiPort: Int, commPort: Int,
    masterUrl: String, workDir: String, slaveNumber: Option[Int] = None): (ActorSystem, Int) = {
    // The LocalVarysCluster runs multiple local varysSlaveX actor systems
    val systemName = "varysSlave" + slaveNumber.map(_.toString).getOrElse("")
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(systemName, host, port)
    val actor = actorSystem.actorOf(Props(new SlaveActor(host, boundPort, webUiPort, commPort,
      masterUrl, workDir)), name = "Slave")
    (actorSystem, boundPort)
  }

}
