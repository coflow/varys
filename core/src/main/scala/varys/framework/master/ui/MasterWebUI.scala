/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package varys.framework.master.ui

import akka.actor._

import javax.servlet.http.HttpServletRequest

import org.eclipse.jetty.server.{Handler, Server}

import scala.concurrent.duration.Duration

import varys.{Logging, Utils}
import varys.ui.JettyUtils
import varys.ui.JettyUtils._

/**
 * Web UI server for the standalone master.
 */
private[varys]
class MasterWebUI(masterActorRef_ : ActorRef, requestedPort: Int) extends Logging {
  implicit val timeout = Duration.create(
    System.getProperty("varys.akka.askTimeout", "10").toLong, "seconds")
  val host = Utils.localHostName()
  val port = requestedPort

  val masterActorRef = masterActorRef_
  
  var server: Option[Server] = None
  var boundPort: Option[Int] = None

  val coflowPage = new CoflowPage(this)
  val indexPage = new IndexPage(this)

  def start() {
    try {
      val (srv, bPort) = JettyUtils.startJettyServer("0.0.0.0", port, handlers)
      server = Some(srv)
      boundPort = Some(bPort)
      logInfo("Started Master web UI at http://%s:%d".format(host, boundPort.get))
    } catch {
      case e: Exception =>
        logError("Failed to create Master JettyUtils", e)
        System.exit(1)
    }
  }

  val handlers = Array[(String, Handler)](
    ("/static", createStaticHandler(MasterWebUI.STATIC_RESOURCE_DIR)),
    ("/coflow/json", (request: HttpServletRequest) => coflowPage.renderJson(request)),
    ("/coflow", (request: HttpServletRequest) => coflowPage.render(request)),
    ("/json", (request: HttpServletRequest) => indexPage.renderJson(request)),
    ("*", (request: HttpServletRequest) => indexPage.render(request))
  )

  def stop() {
    server.foreach(_.stop())
  }
}

private[varys] object MasterWebUI {
  val STATIC_RESOURCE_DIR = "varys/ui/static"
}
