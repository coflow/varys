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

import akka.pattern.ask

import javax.servlet.http.HttpServletRequest

import net.liftweb.json.JsonAST.JValue

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.xml.Node

import varys.framework.{MasterState, RequestMasterState}
import varys.framework.JsonProtocol
import varys.framework.ClientInfo
import varys.ui.UIUtils
import varys.Utils

private[varys] class CoflowPage(parent: MasterWebUI) {
  val master = parent.masterActorRef
  implicit val timeout = parent.timeout

  /** Details for a particular Coflow */
  def renderJson(request: HttpServletRequest): JValue = {
    val coflowId = request.getParameter("coflowId")
    val stateFuture = (master ? RequestMasterState)(timeout).mapTo[MasterState]
    val state = Await.result(stateFuture, 30.seconds)
    val coflow = state.activeCoflows.find(_.id == coflowId.toInt).getOrElse({
      state.completedCoflows.find(_.id == coflowId.toInt).getOrElse(null)
    })
    JsonProtocol.writeCoflowInfo(coflow)
  }

  /** Details for a particular Coflow */
  def render(request: HttpServletRequest): Seq[Node] = {
    val coflowId = request.getParameter("coflowId")
    val stateFuture = (master ? RequestMasterState)(timeout).mapTo[MasterState]
    val state = Await.result(stateFuture, 30.seconds)
    val coflow = state.activeCoflows.find(_.id == coflowId.toInt).getOrElse({
      state.completedCoflows.find(_.id == coflowId.toInt).getOrElse(null)
    })

    val content =
        <div class="row-fluid">
          <div class="span12">
            <ul class="unstyled">
              <li><strong>ID:</strong> {coflow.id}</li>
              <li><strong>Name:</strong> {coflow.desc.name}</li>
              <li><strong>User:</strong> {coflow.desc.user}</li>
              <li><strong>Submit Date:</strong> {coflow.submitDate}</li>
              <li><strong>State:</strong> {coflow.curState}</li>
            </ul>
          </div>
        </div>
    UIUtils.basicVarysPage(content, "Coflow: " + coflow.desc.name)
  }
}
