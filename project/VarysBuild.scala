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

  // A configuration to set an alternative publishLocalConfiguration
  lazy val MavenCompile = config("m2r") extend(Compile)
  lazy val publishLocalBoth = TaskKey[Unit]("publish-local", "publish local for m2 and ivy")

  def sharedSettings = Defaults.defaultSettings ++ Seq(
    organization := "org.varys",
    version := "0.0.1",
    scalaVersion := "2.9.2",
    scalacOptions := Seq("-deprecation", "-unchecked", "-optimize"),
    unmanagedJars in Compile <<= baseDirectory map { base => (base / "lib" ** "*.jar").classpath },
    retrieveManaged := true,
    retrievePattern := "[type]s/[artifact](-[revision])(-[classifier]).[ext]",
    transitiveClassifiers in Scope.GlobalScope := Seq("sources"),

    publishMavenStyle := true,

    pomExtra := (
      <url>http://varys-project.org/</url>
      <licenses>
        <license>
          <name>BSD License</name>
          <url>https://github.com/mesos/varys/blob/master/LICENSE</url>
          <distribution>repo</distribution>
        </license>
      </licenses>
      <scm>
        <connection>scm:git:git@bitbucket.org:mosharaf/varys.git</connection>
        <url>scm:git:git@bitbucket.org:mosharaf/varys.git</url>
      </scm>
      <developers>
        <developer>
          <id>matei</id>
          <name>Mosharaf Chowdhury</name>
          <email>mosharafkabir@gmail.com</email>
          <url>http://www.mosharaf.com</url>
          <organization>U.C. Berkeley Computer Science</organization>
          <organizationUrl>http://www.cs.berkeley.edu</organizationUrl>
        </developer>
      </developers>
    ),

    libraryDependencies ++= Seq(
      // "org.eclipse.jetty" % "jetty-server" % "7.5.3.v20111011"
    ),
    
    parallelExecution := false,
    /* Workaround for issue #206 (fixed after SBT 0.11.0) */
    watchTransitiveSources <<= Defaults.inDependencies[Task[Seq[File]]](watchSources.task,
      const(std.TaskExtra.constant(Nil)), aggregate = true, includeRoot = true) apply { _.join.map(_.flatten) },

    otherResolvers := Seq(Resolver.file("dotM2", file(Path.userHome + "/.m2/repository"))),
    publishLocalConfiguration in MavenCompile <<= (packagedArtifacts, deliverLocal, ivyLoggingLevel) map {
      (arts, _, level) => new PublishConfiguration(None, "dotM2", arts, Seq(), level)
    },
    publishMavenStyle in MavenCompile := true,
    publishLocal in MavenCompile <<= publishTask(publishLocalConfiguration in MavenCompile, deliverLocal),
    publishLocalBoth <<= Seq(publishLocal in MavenCompile, publishLocal).dependOn
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
    )) ++ assemblySettings ++ extraAssemblySettings ++ Twirl.settings ++ ThriftPlugin.thriftSettings

  def rootSettings = sharedSettings ++ Seq(
    publish := {}
  )

  def extraAssemblySettings() = Seq(test in assembly := {}) ++ Seq(
    mergeStrategy in assembly := {
      case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  )
}
