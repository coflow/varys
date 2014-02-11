package varys

import com.google.common.io.Files
import com.google.common.util.concurrent.ThreadFactoryBuilder

import java.io._
import java.net._
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.util.{Locale, Random, UUID}
import java.util.concurrent.{Executors, ThreadFactory, ThreadPoolExecutor}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.io.Source
import scala.Some

import sun.nio.ch.DirectBuffer

/**
 * Various utility methods used by Varys.
 */
private object Utils extends Logging {
  
  /** 
   * Serialize an object using Java serialization 
   */
  def serialize[T](o: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
    return bos.toByteArray
  }

  /** 
   * Deserialize an object using Java serialization 
   */
  def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    return ois.readObject.asInstanceOf[T]
  }

  /** 
   * Deserialize an object using Java serialization and the given ClassLoader 
   */
  def deserialize[T](bytes: Array[Byte], loader: ClassLoader): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis) {
      override def resolveClass(desc: ObjectStreamClass) =
        Class.forName(desc.getName, false, loader)
    }
    return ois.readObject.asInstanceOf[T]
  }

  def isAlpha(c: Char): Boolean = {
    (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
  }

  /** 
   * Split a string into words at non-alphabetic characters 
   */
  def splitWords(s: String): Seq[String] = {
    val buf = new ArrayBuffer[String]
    var i = 0
    while (i < s.length) {
      var j = i
      while (j < s.length && isAlpha(s.charAt(j))) {
        j += 1
      }
      if (j > i) {
        buf += s.substring(i, j)
      }
      i = j
      while (i < s.length && !isAlpha(s.charAt(i))) {
        i += 1
      }
    }
    return buf
  }

  /** 
   * Create a temporary directory inside the given parent directory 
   */
  def createTempDir(root: String = System.getProperty("java.io.tmpdir")): File = {
    var attempts = 0
    val maxAttempts = 10
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory after " + maxAttempts + 
          " attempts!")
      }
      try {
        dir = new File(root, "varys-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch { case e: IOException => ; }
    }
    // Add a shutdown hook to delete the temp dir when the JVM exits
    Runtime.getRuntime.addShutdownHook(new Thread("delete Varys temp dir " + dir) {
      override def run() {
        Utils.deleteRecursively(dir)
      }
    })
    return dir
  }

  /**
   * Get a temporary directory using Varys's varys.local.dir property, if set. This will always
   * return a single directory, even though the varys.local.dir property might be a list of
   * multiple paths.
   */
  def getLocalDir: String = {
    System.getProperty("varys.local.dir", System.getProperty("java.io.tmpdir")).split(',')(0)
  }

  /**
   * Shuffle the elements of a collection into a random order, returning the
   * result in a new collection. Unlike scala.util.Random.shuffle, this method
   * uses a local random number generator, avoiding inter-thread contention.
   */
  def randomize[T: ClassManifest](seq: TraversableOnce[T]): Seq[T] = {
    randomizeInPlace(seq.toArray)
  }

  /**
   * Shuffle the elements of an array into a random order, modifying the
   * original array. Returns the original array.
   */
  def randomizeInPlace[T](arr: Array[T], rand: Random = new Random): Array[T] = {
    for (i <- (arr.length - 1) to 1 by -1) {
      val j = rand.nextInt(i)
      val tmp = arr(j)
      arr(j) = arr(i)
      arr(i) = tmp
    }
    arr
  }

  /**
   * Get the local host's IP address in dotted-quad format (e.g. 1.2.3.4).
   */
  lazy val localIpAddress: String = findLocalIpAddress()

  private def findLocalIpAddress(): String = {
    val defaultIpOverride = System.getenv("VARYS_LOCAL_IP")
    if (defaultIpOverride != null) {
      defaultIpOverride
    } else {
      val address = InetAddress.getLocalHost
      if (address.isLoopbackAddress) {
        // Address resolves to something like 127.0.1.1, which happens on Debian; try to find
        // a better address using the local network interfaces
        for (ni <- NetworkInterface.getNetworkInterfaces) {
          for (addr <- ni.getInetAddresses if !addr.isLinkLocalAddress && 
              !addr.isLoopbackAddress && addr.isInstanceOf[Inet4Address]) {

            // We've found an address that looks reasonable!
            logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
              " a loopback address: " + address.getHostAddress + "; using " + addr.getHostAddress +
              " instead (on interface " + ni.getName + ")")
            logWarning("Set VARYS_LOCAL_IP if you need to bind to another address")
            return addr.getHostAddress
          }
        }
        logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
          " a loopback address: " + address.getHostAddress + ", but we couldn't find any" +
          " external IP address!")
        logWarning("Set VARYS_LOCAL_IP if you need to bind to another address")
      }
      address.getHostAddress
    }
  }

  private var customHostname: Option[String] = None

  /**
   * Allow setting a custom host name because when we run on Mesos we need to use the same
   * hostname it reports to the master.
   */
  def setCustomHostname(hostname: String) {
    customHostname = Some(hostname)
  }

  /**
   * Get the local machine's hostname.
   */
  def localHostName(): String = {
    customHostname.getOrElse(InetAddress.getLocalHost.getHostName)
  }

  private[varys] val daemonThreadFactory: ThreadFactory =
    new ThreadFactoryBuilder().setDaemon(true).build()

  /**
   * Wrapper over newCachedThreadPool.
   */
  def newDaemonCachedThreadPool(): ThreadPoolExecutor =
    Executors.newCachedThreadPool(daemonThreadFactory).asInstanceOf[ThreadPoolExecutor]

  /**
   * Return the string to tell how long has passed in seconds. The passing parameter should be in
   * millisecond.
   */
  def getUsedTimeMs(startTimeMs: Long): String = {
    return " " + (System.currentTimeMillis - startTimeMs) + " ms"
  }

  /**
   * Wrapper over newFixedThreadPool.
   */
  def newDaemonFixedThreadPool(nThreads: Int): ThreadPoolExecutor =
    Executors.newFixedThreadPool(nThreads, daemonThreadFactory).asInstanceOf[ThreadPoolExecutor]

  /**
   * Delete a file or directory and its contents recursively.
   */
  def deleteRecursively(file: File) {
    if (file.isDirectory) {
      for (child <- file.listFiles()) {
        deleteRecursively(child)
      }
    }
    if (!file.delete()) {
      throw new IOException("Failed to delete: " + file)
    }
  }

  /**
   * Execute a command in the given working directory, throwing an exception if it completes
   * with an exit code other than 0.
   */
  def execute(command: Seq[String], workingDir: File) {
    val process = new ProcessBuilder(command: _*)
        .directory(workingDir)
        .redirectErrorStream(true)
        .start()
    new Thread("read stdout for " + command(0)) {
      override def run() {
        for (line <- Source.fromInputStream(process.getInputStream).getLines) {
          System.err.println(line)
        }
      }
    }.start()
    val exitCode = process.waitFor()
    if (exitCode != 0) {
      throw new VarysException("Process " + command + " exited with code " + exitCode)
    }
  }

  /**
   * Execute a command in the current working directory, throwing an exception if it completes
   * with an exit code other than 0.
   */
  def execute(command: Seq[String]) {
    execute(command, new File("."))
  }

  /**
   * Try to find a free port to bind to on the local host. This should ideally never be needed,
   * except that, unfortunately, some of the networking libraries we currently rely on (e.g. Spray)
   * don't let users bind to port 0 and then figure out which free port they actually bound to.
   * We work around this by binding a ServerSocket and immediately unbinding it. This is *not*
   * necessarily guaranteed to work, but it's the best we can do.
   */
  def findFreePort(): Int = {
    val socket = new ServerSocket(0)
    val portBound = socket.getLocalPort
    socket.close()
    portBound
  }
  
  /**
   * Attempt to clean up a ByteBuffer if it is memory-mapped. This uses an *unsafe* Sun API that
   * might cause errors if one attempts to read from the unmapped buffer, but it's better than
   * waiting for the GC to find it because that could lead to huge numbers of open files. There's
   * unfortunately no standard API to do this.
   */
  def dispose(buffer: ByteBuffer) {
    if (buffer != null && buffer.isInstanceOf[MappedByteBuffer]) {
      logTrace("Unmapping " + buffer)
      if (buffer.asInstanceOf[DirectBuffer].cleaner() != null) {
        buffer.asInstanceOf[DirectBuffer].cleaner().clean()
      }
    }
  }
  
}
