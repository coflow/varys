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

package varys.ui

import javax.servlet.http.HttpServletRequest

import org.eclipse.jetty.server.{Handler, Server}

import varys.{Logging, Utils}
import varys.ui.env.EnvironmentUI
import varys.ui.JettyUtils._

/** Top level user interface for Varys */
private[varys] class VarysUI() extends Logging {
  val host = Option(System.getenv("VARYS_PUBLIC_DNS")).getOrElse(Utils.localHostName())
  val port = Option(System.getProperty("varys.ui.port")).getOrElse(VarysUI.DEFAULT_PORT).toInt
  var boundPort: Option[Int] = None
  var server: Option[Server] = None

  val handlers = Seq[(String, Handler)](
    ("/static", createStaticHandler(VarysUI.STATIC_RESOURCE_DIR)),
    ("/", createRedirectHandler("/stages"))
  )
  val env = new EnvironmentUI()

  val allHandlers = env.getHandlers ++ handlers

  /** Bind the HTTP server which backs this web interface */
  def bind() {
    try {
      val (srv, usedPort) = JettyUtils.startJettyServer("0.0.0.0", port, allHandlers)
      logInfo("Started Varys Web UI at http://%s:%d".format(host, usedPort))
      server = Some(srv)
      boundPort = Some(usedPort)
    } catch {
      case e: Exception =>
        logError("Failed to create Varys JettyUtils", e)
        System.exit(1)
    }
  }

  /** Initialize all components of the server */
  def start() {
    // NOTE: This is decoupled from bind() because of the following dependency cycle:
    // DAGScheduler() requires that the port of this server is known
    // This server must register all handlers, including CoflowProgressUI, before binding
    // CoflowProgressUI registers a listener with VarysContext, which requires sc to initialize
  }

  def stop() {
    server.foreach(_.stop())
  }

  private[varys] def appUIAddress = host + ":" + boundPort.getOrElse("-1")

}

private[varys] object VarysUI {
  val DEFAULT_PORT = "4040"
  val STATIC_RESOURCE_DIR = "varys/ui/static"
}
