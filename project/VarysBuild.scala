import sbt._
import sbt.Classpaths.publishTask
import Keys._
import Classpaths.managedJars

object VarysBuild extends Build {
  lazy val root = Project("root", file("."), settings = rootSettings) aggregate(core, examples)

  lazy val core = Project("core", file("core"), settings = coreSettings)

  lazy val examples = Project("examples", file("examples"), settings = examplesSettings) dependsOn (core)

  lazy val jarsToExtract = TaskKey[Seq[File]]("jars-to-extract", "JAR files to be extracted")

  lazy val extractJarsTarget = SettingKey[File]("extract-jars-target", "Target directory for extracted JAR files")

  lazy val extractJars = TaskKey[Unit]("extract-jars", "Extracts JAR files")

  def sharedSettings = Defaults.defaultSettings ++ Seq(
    organization := "net.varys",
    version := "0.2.0-SNAPSHOT",
    scalaVersion := "2.10.4",
    scalacOptions := Seq("-deprecation", "-unchecked", "-optimize", "-feature"),
    unmanagedJars in Compile <<= baseDirectory map { base => (base / "lib" ** "*.jar").classpath },
    retrieveManaged := true,
    retrievePattern := "[type]s/[artifact](-[revision])(-[classifier]).[ext]",
    transitiveClassifiers in Scope.GlobalScope := Seq("sources"),

    parallelExecution := false,
    
    libraryDependencies ++= Seq(
        "org.eclipse.jetty" % "jetty-server"   % jettyVersion
    )
  )

  val jettyVersion = "8.1.14.v20131031"
  val slf4jVersion = "1.7.5"
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
      "log4j" % "log4j" % "1.2.17",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
      "org.apache.commons" % "commons-io" % "1.3.2",
      "com.typesafe.akka" %% "akka-actor" % "2.2.3" excludeAll(excludeNetty),
      "com.typesafe.akka" %% "akka-remote" % "2.2.3" excludeAll(excludeNetty),
      "com.typesafe.akka" %% "akka-slf4j" % "2.2.3" excludeAll(excludeNetty),
      "net.liftweb" %% "lift-json" % "2.5.1",
      "io.netty" % "netty-all" % "4.0.23.Final",
      "org.fusesource" % "sigar" % sigarVersion classifier "" classifier "native",
      "com.esotericsoftware.kryo" % "kryo" % "2.19",
      "javax.servlet" % "javax.servlet-api" % "3.0.1",
      "com.github.romix.akka" % "akka-kryo-serialization_2.10" % "0.3.1",
      "net.openhft" % "chronicle" % "3.2.1"
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
  )

  def rootSettings = sharedSettings ++ Seq(
    publish := {}
  )

  def examplesSettings = sharedSettings ++ Seq(
    name := "varys-examples"
  )

}
