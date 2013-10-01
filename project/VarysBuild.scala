import sbt._
import sbt.Classpaths.publishTask
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._
import com.github.bigtoast.sbtthrift.ThriftPlugin
import Classpaths.managedJars

object VarysBuild extends Build {
  lazy val root = Project("root", file("."), settings = rootSettings) aggregate(core, examples)

  lazy val core = Project("core", file("core"), settings = coreSettings)

  lazy val examples = Project("examples", file("examples"), settings = examplesSettings) dependsOn (core)

  lazy val jarsToExtract = TaskKey[Seq[File]]("jars-to-extract", "JAR files to be extracted")

  lazy val extractJarsTarget = SettingKey[File]("extract-jars-target", "Target directory for extracted JAR files")

  lazy val extractJars = TaskKey[Unit]("extract-jars", "Extracts JAR files")

  def sharedSettings = Defaults.defaultSettings ++ Seq(
    organization := "org.varys-project",
    version := "0.0.1",
    scalaVersion := "2.9.3",
    scalacOptions := Seq("-deprecation", "-unchecked", "-optimize"),
    unmanagedJars in Compile <<= baseDirectory map { base => (base / "lib" ** "*.jar").classpath },
    retrieveManaged := true,
    retrievePattern := "[type]s/[artifact](-[revision])(-[classifier]).[ext]",
    transitiveClassifiers in Scope.GlobalScope := Seq("sources"),

    parallelExecution := false,
    
    libraryDependencies ++= Seq(
      "org.eclipse.jetty" % "jetty-server" % "7.6.8.v20121106"
    )
  )

  val slf4jVersion = "1.6.1"
  val sigarVersion = "1.6.4"

  val excludeNetty = ExclusionRule(organization = "org.jboss.netty")

  def coreSettings = sharedSettings ++ Seq(
    name := "varys-core",
    resolvers ++= Seq(
      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
      "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
      "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
      "Twitter4J Repository" at "http://twitter4j.org/maven2/"
    ),

    libraryDependencies ++= Seq(
      "com.google.guava" % "guava" % "11.0.1",
      "log4j" % "log4j" % "1.2.16",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
      "com.google.protobuf" % "protobuf-java" % "2.4.1",
      "com.typesafe.akka" % "akka-actor" % "2.0.3" excludeAll(excludeNetty),
      "com.typesafe.akka" % "akka-remote" % "2.0.3" excludeAll(excludeNetty),
      "com.typesafe.akka" % "akka-slf4j" % "2.0.3" excludeAll(excludeNetty),
      "net.liftweb" % "lift-json_2.9.2" % "2.5",
      "org.apache.thrift" % "libthrift" % "0.8.0",
      "io.netty" % "netty-all" % "4.0.0.Beta2",
      "org.fusesource" % "sigar" % sigarVersion classifier "" classifier "native",
      "com.esotericsoftware.kryo" % "kryo" % "2.19",
      // akka-kryo-serialization has been added in an hackish way. We've compiled locally, then uploaded the jar to my website.
      "akka-kryo-serialization" % "akka-kryo-serialization" % "0.2-SNAPSHOT" from "http://mosharaf.com/akka-kryo-serialization-0.2-SNAPSHOT.jar"
    ),
    
    // Collect jar files to be extracted from managed jar dependencies
    jarsToExtract <<= (classpathTypes, update) map { (ct, up) =>
      managedJars(Compile, ct, up) map { _.data } filter { _.getName.startsWith("sigar-" + sigarVersion + "-native") }
    },

    // Define the target directory
    extractJarsTarget <<= (baseDirectory)(_ / "../lib_managed/jars"),
    
    // Task to extract jar files
    extractJars <<= (jarsToExtract, extractJarsTarget, streams) map { (jars, target, streams) =>
      jars foreach { jar =>
        streams.log.info("Extracting " + jar.getName + " to " + target)
        IO.unzip(jar, target)
      }
    },
    
    // Make extractJars run before compile
    (compile in Compile) <<= (compile in Compile) dependsOn(extractJars)
  ) ++ assemblySettings ++ ThriftPlugin.thriftSettings

  def rootSettings = sharedSettings ++ Seq(
    publish := {}
  )

  def examplesSettings = sharedSettings ++ Seq(
    name := "varys-examples"
  )

}
