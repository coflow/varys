import sbt._
import sbt.Classpaths.publishTask
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._
import twirl.sbt.TwirlPlugin._
import com.github.bigtoast.sbtthrift.ThriftPlugin

object VarysBuild extends Build {
  lazy val root = Project("root", file("."), settings = rootSettings) aggregate(core)

  lazy val core = Project("core", file("core"), settings = coreSettings)

  def sharedSettings = Defaults.defaultSettings ++ Seq(
    organization := "org.varys-project",
    version := "0.0.1",
    scalaVersion := "2.9.2",
    scalacOptions := Seq("-deprecation", "-unchecked", "-optimize"),
    unmanagedJars in Compile <<= baseDirectory map { base => (base / "lib" ** "*.jar").classpath },
    retrieveManaged := true,
    retrievePattern := "[type]s/[artifact](-[revision])(-[classifier]).[ext]",
    transitiveClassifiers in Scope.GlobalScope := Seq("sources"),

    parallelExecution := false
    
  )

  val slf4jVersion = "1.6.1"

  def coreSettings = sharedSettings ++ Seq(
    name := "varys-core",
    resolvers ++= Seq(
      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
      "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
      "Spray Repository" at "http://repo.spray.cc/",
      "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
      "Twitter4J Repository" at "http://twitter4j.org/maven2/"
    ),

    libraryDependencies ++= Seq(
      "com.google.guava" % "guava" % "11.0.1",
      "log4j" % "log4j" % "1.2.16",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
      "com.typesafe.akka" % "akka-actor" % "2.0.3",
      "com.typesafe.akka" % "akka-remote" % "2.0.3",
      "com.typesafe.akka" % "akka-slf4j" % "2.0.3",
      "cc.spray" % "spray-can" % "1.0-M2.1",
      "cc.spray" % "spray-server" % "1.0-M2.1",
      "cc.spray" %%  "spray-json" % "1.1.1",
      "org.apache.thrift" % "libthrift" % "0.8.0"
    )) ++ assemblySettings ++ Twirl.settings ++ ThriftPlugin.thriftSettings

  def rootSettings = sharedSettings ++ Seq(
    publish := {}
  )

}
