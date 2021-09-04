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

package org.apache.spark.deploy.shuffleService.ui

import javax.servlet.http.HttpServletRequest
import org.apache.spark.deploy.DeployMessages.{RequestShuffleServiceState, ShuffleServiceStateResponse}
import org.apache.spark.deploy.JsonProtocol
import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.json4s.JValue

import scala.xml.Node

private[ui] class ShuffleServicePage(parent: ShuffleServiceWebUI) extends WebUIPage("") {
  private val shuffleServiceEndpoint = parent.shuffleService.self

  override def renderJson(request: HttpServletRequest): JValue = {
    val serviceState = shuffleServiceEndpoint.askSync[ShuffleServiceStateResponse](RequestShuffleServiceState)
    JsonProtocol.writeShuffleServiceState(serviceState)
  }

  /*private def formatWorkerResourcesDetails(shuffleServiceState: ShuffleServiceStateResponse): String = {
    val totalInfo = shuffleServiceState.resources
    val usedInfo = shuffleServiceState.resourcesUsed
    val freeInfo = totalInfo.map { case (rName, rInfo) =>
      val freeAddresses = if (usedInfo.contains(rName)) {
        rInfo.addresses.diff(usedInfo(rName).addresses)
      } else {
        rInfo.addresses
      }
      rName -> new ResourceInformation(rName, freeAddresses)
    }
    formatResourcesDetails(usedInfo, freeInfo)
  }*/

  def render(request: HttpServletRequest): Seq[Node] = {
    val shuffleServiceState = shuffleServiceEndpoint.askSync[ShuffleServiceStateResponse](RequestShuffleServiceState)

    /*val executorHeaders = Seq("ExecutorID", "State", "Cores", "Memory", "Resources",
      "Job Details", "Logs")
    val runningExecutors = shuffleServiceState.executors
    val runningExecutorTable =
      UIUtils.listingTable(executorHeaders, executorRow, runningExecutors)
    val finishedExecutors = shuffleServiceState.finishedExecutors
    val finishedExecutorTable =
      UIUtils.listingTable(executorHeaders, executorRow, finishedExecutors)*/

    val blockTransferRateBytes = shuffleServiceState.blockTransferRateBytes
    val registeredExecutorsSize = shuffleServiceState.registeredExecutorsSize
    val numActiveConnections = shuffleServiceState.numActiveConnections
    val numCaughtExceptions = shuffleServiceState.numCaughtExceptions

    // For now we only show driver information if the user has submitted drivers to the cluster.
    // This is until we integrate the notion of drivers and applications in the UI.

    val content =
      <div class="row"> <!-- Worker Details -->
        <div class="col-12">
          <ul class="list-unstyled">
            <li><strong>block Transfer Rate Bytes:</strong> {blockTransferRateBytes}</li>
            <li><strong>registered Executors Size:</strong> {registeredExecutorsSize}</li>
            <li><strong>num Active Connections:</strong> {numActiveConnections}</li>
            <li><strong>num Caught Exceptions:</strong> {numCaughtExceptions}</li>
            <!--<li>
              <strong> Master URL:</strong> {shuffleServiceState.masterUrl}
            </li>
            <li><strong>Cores:</strong> {shuffleServiceState.cores}</li>
            <li><strong>Memory:</strong> {Utils.megabytesToString(shuffleServiceState.memory)}</li>
            <li><strong>Resources:</strong>
              {formatWorkerResourcesDetails(shuffleServiceState)}</li> -->
          </ul>
        </div>
      </div>
      <div class="row"> <!-- Executors and Drivers -->
        <!--  <div class="col-12">
          <span class="collapse-aggregated-runningExecutors collapse-table"
              onClick="collapseTable('collapse-aggregated-runningExecutors',
              'aggregated-runningExecutors')">
            <h4>
              <span class="collapse-table-arrow arrow-open"></span>
              <a>Connected Workers ({runningExecutors.number})</a>
            </h4>
          </span>

        </div>-->
      </div>;
    UIUtils.basicSparkPage(request, content, "Spark Shuffle Service at %s:%s".format(
      shuffleServiceState.host, shuffleServiceState.port))
  }

  /*def executorRow(executor: ExecutorRunner): Seq[Node] = {
    val workerUrlRef = UIUtils.makeHref(parent.worker.reverseProxy, executor.workerId,
      parent.webUrl)
    val appUrlRef = UIUtils.makeHref(parent.worker.reverseProxy, executor.appId,
      executor.appDesc.appUiUrl)

    <tr>
      <td>{executor.execId}</td>
      <td>{executor.state}</td>
      <td>{executor.cores}</td>
      <td sorttable_customkey={executor.memory.toString}>
        {Utils.megabytesToString(executor.memory)}
      </td>
      <td>{formatResourcesAddresses(executor.resources)}</td>
      <td>
        <ul class="list-unstyled">
          <li><strong>ID:</strong> {executor.appId}</li>
          <li><strong>Name:</strong>
          {
            if ({executor.state == ExecutorState.RUNNING} && executor.appDesc.appUiUrl.nonEmpty) {
              <a href={appUrlRef}> {executor.appDesc.name}</a>
            } else {
              {executor.appDesc.name}
            }
          }
          </li>
          <li><strong>User:</strong> {executor.appDesc.user}</li>
        </ul>
      </td>
      <td>
        <a href={s"$workerUrlRef/logPage?appId=${executor
          .appId}&executorId=${executor.execId}&logType=stdout"}>stdout</a>
        <a href={s"$workerUrlRef/logPage?appId=${executor
          .appId}&executorId=${executor.execId}&logType=stderr"}>stderr</a>
      </td>
    </tr>

  }*/
}
