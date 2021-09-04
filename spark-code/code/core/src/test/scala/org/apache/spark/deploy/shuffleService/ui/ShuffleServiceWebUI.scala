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
import org.apache.spark.deploy.shuffleService.CherryServiceRunner
import org.apache.spark.internal.Logging
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.ui.{SparkUI, WebUI}
import org.apache.spark.util.RpcUtils

/**
 * Web UI server for the external shuffle service.
 */
private[shuffleService]
class ShuffleServiceWebUI(
    val shuffleService: CherryServiceRunner,
    requestedPort: Int)
  extends WebUI(shuffleService.securityManager, shuffleService.securityManager.getSSLOptions("standalone"),
    requestedPort, shuffleService.sparkConf, name = "ShuffleServiceUI")
  with Logging {

  private[ui] val timeout = RpcUtils.askRpcTimeout(shuffleService.sparkConf)

  initialize()

  /** Initialize all components of the server. */
  def initialize(): Unit = {
    val logPage = new LogPage(this)
    attachPage(logPage)
    attachPage(new ShuffleServicePage(this))
    addStaticHandler(ShuffleServiceWebUI.STATIC_RESOURCE_BASE)
    attachHandler(createServletHandler("/log",
      (request: HttpServletRequest) => logPage.renderLog(request),
      shuffleService.sparkConf))
  }
}

private[shuffleService] object ShuffleServiceWebUI {
  val STATIC_RESOURCE_BASE = SparkUI.STATIC_RESOURCE_DIR
}
