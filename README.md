# Varys
Varys is an open-source application-aware network scheduler that aims to improve communication performance of Big Data applications. Its target applications/jobs include those written in Spark, Hadoop, YARN, BSP, and similar data-parallel frameworks.

Varys provides a simple API that allows data-parallel frameworks to express their communication requirements as coflows with minimal changes to the framework. **User jobs do not require any modification**. Using coflows as the basic abstraction of network scheduling, Varys implements novel schedulers to make applications faster.

To learn more, visit <http://varys.net/>

## Building Varys
Varys is built on `Scala 2.10`. To build Varys and example programs using it, run

	./sbt/sbt package

## Using Varys
Just wrap your sockets with `VarysInputStream` and `VarysOutputStream` before using whatever `InputStream` or `OutputStream` you want. That's ALL!

## Dependency Information
Currently, Varys has not yet been published to any repository. The easiest way to use it in your project is to first publish it to local ivy repository. 

	./sbt/sbt publish-local

Other projects on the same machine can then list the project as a dependency. 

### SBT
```
libraryDependencies += "net.varys" %% "varys-core" % "0.2.0-SNAPSHOT"
```

### Apache Maven
```xml
<dependency>
  <groupId>net.varys</groupId>
  <artifactId>varys-core</artifactId>
  <version>0.2.0-SNAPSHOT</version>
</dependency>
```

### Apache Ant
```xml
<dependency org="net.varys" name="varys-core" rev="0.2.0-SNAPSHOT">
  <artifact name="varys-core" type="jar" />
</dependency>
```

Directly putting the jar in your classpath should also work. It is located at `./core/target/scala-*/varys-core*.jar`.

## Example Programs
Varys also comes with several sample programs in the `examples` directory. For example, the `VarysExampleServer` and `VarysExampleClient` programs are typical TCP server and client showing how to use coflows. 

To run them in your local machine, first start Varys by typing

	./bin/start-all.sh

Now, go to <http://localhost:16016/> in your browser to find the MASTER_URL.

Next, start the sender and the receiver

	./run varys.examples.VarysExampleServer <MASTER_URL> <serverPort>
	./run varys.examples.VarysExampleClient <serverHost> <serverPort> 0

This should log statements on the console stating coflow register, transfer, and other events. 

Finally, stop Varys by typing

	./bin/stop-all.sh

Note that we are manually providing the CoflowId (0) to the reciever, which should be provided by the framework driver (e.g., JobTracker for Hadoop or SparkContext for Spark).
Take a look at the `BroadcastService` example that does a slightly better job in containing everything (driver, sender, and receiver) in the same application.

Finally, if we do not start Varys (i.e., do not call `start-all.sh`) then `VarysInputStream` and `VarysOutputStream` will transparently forward data back and forth between sockets and applications.
