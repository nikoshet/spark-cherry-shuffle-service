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

package org.apache.spark.deploy.shuffleService

import java.io.File
import org.apache.spark.deploy.shuffleService.ui.ShuffleServiceWebUI
import org.apache.spark.internal.config.{LOOK_AHEAD_CACHING_ENABLED, LOOK_AHEAD_CACHING_SIZE}
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.metrics.{MetricsSystem, MetricsSystemInstances}
import org.apache.spark.network.TransportContext
import org.apache.spark.network.crypto.AuthServerBootstrap
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.server.{TransportServer, TransportServerBootstrap}
import org.apache.spark.network.shuffle.RemoteBlockHandler
import org.apache.spark.network.util.TransportConf
import org.apache.spark.{SecurityManager, SparkConf}

import scala.collection.JavaConverters._

/**
 * Provides a server from which Executors can read shuffle files (rather than reading directly from
 * each other), to provide uninterrupted access to the files in the face of executors being turned
 * off or killed.
 *
 * Optionally requires SASL authentication in order to read. See [[SecurityManager]].
 */
private[deploy]
class CherryShuffleService(sparkConf: SparkConf, securityManager: SecurityManager)
  extends Logging {
  protected val metricsSystem =
    MetricsSystem.createMetricsSystem(MetricsSystemInstances.SHUFFLE_SERVICE,
      sparkConf, securityManager)

  private val enabled = sparkConf.get(config.SHUFFLE_SERVICE_ENABLED)

  private val host = sparkConf.get(config.SHUFFLE_SERVICE_HOST)
  private val port = sparkConf.get(config.SHUFFLE_SERVICE_PORT)

  private val lookAheadCachingEnabled= sparkConf.get(LOOK_AHEAD_CACHING_ENABLED)
  private val lookAheadCachingSize= sparkConf.get(LOOK_AHEAD_CACHING_SIZE)

  private val registeredExecutorsDB = "registeredExecutors.ldb"

  private val transportConf =
    SparkTransportConf.fromSparkConf(sparkConf, "shuffle", numUsableCores = 0)

  val blockHandler = newShuffleBlockHandler(sparkConf, transportConf, lookAheadCachingEnabled, lookAheadCachingSize) //private
  private var transportContext: TransportContext = _

  private var server: TransportServer = _

  private val shuffleServiceSource = new ExternalShuffleServiceSource

  protected def findRegisteredExecutorsDBFile(dbName: String): File = {
    val localDirs = sparkConf.getOption("spark.local.dir").map(_.split(",")).getOrElse(Array())
    if (localDirs.length >= 1) {
      new File(localDirs.find(new File(_, dbName).exists()).getOrElse(localDirs(0)), dbName)
    } else {
      logWarning(s"'spark.local.dir' should be set first when we use db in " +
        s"ExternalShuffleService. Note that this only affects standalone mode.")
      null
    }
  }

  /** Get blockhandler  */
  def getBlockHandler: RemoteBlockHandler = {
    blockHandler
  }

  /** Create a new shuffle block handler. Factored out for subclasses to override. */
  protected def newShuffleBlockHandler(sparkConf: SparkConf, conf: TransportConf, lookAheadCachingEnabled: Boolean, lookAheadCachingSize: Int): RemoteBlockHandler = {
    if (sparkConf.get(config.SHUFFLE_SERVICE_DB_ENABLED) && enabled) {
      new RemoteBlockHandler(conf, findRegisteredExecutorsDBFile(registeredExecutorsDB), lookAheadCachingEnabled, lookAheadCachingSize)
    } else {
      new RemoteBlockHandler(conf, null, lookAheadCachingEnabled, lookAheadCachingSize)
    }
  }

  private var shuffleServiceWebUiUrl: String = ""
  private val publicAddress = {
    val envVar = sparkConf.getenv("SPARK_PUBLIC_DNS")
    //if (envVar != null) envVar else host
    envVar
  }
  /** Starts the web interface for external shuffle service */
  def startWebInterface(metricsSystem: MetricsSystem, webUi: ShuffleServiceWebUI): Unit = {
    webUi.bind()
    shuffleServiceWebUiUrl = s"${webUi.scheme}$publicAddress:${webUi.boundPort}"
    // Attach the shuffle service metrics servlet handler to the web ui after the metrics system is started.
    metricsSystem.getServletHandlers.foreach(webUi.attachHandler)
  }

  /** Starts the external shuffle service if the user has configured us to. */
  def startIfEnabled(): Unit = {
    if (enabled) {
      val webUi = null
      start(webUi: ShuffleServiceWebUI)
    }
  }

  /** Start the external shuffle service */
  def start(webUi: ShuffleServiceWebUI): Unit = {
    require(server == null, "Shuffle server already started")
    val authEnabled = securityManager.isAuthenticationEnabled()
    logInfo(s"Starting shuffle service on host $host")
    logInfo(s"Starting shuffle service on port $port (auth enabled = $authEnabled)")
    val bootstraps: Seq[TransportServerBootstrap] =
      if (authEnabled) {
        Seq(new AuthServerBootstrap(transportConf, securityManager))
      } else {
        Nil
      }
    transportContext = new TransportContext(transportConf, blockHandler, true)
    server = transportContext.createServer(host, port, bootstraps.asJava)

    shuffleServiceSource.registerMetricSet(server.getAllMetrics)
    blockHandler.getAllMetrics.getMetrics.put("numRegisteredConnections",
        server.getRegisteredConnections)
    shuffleServiceSource.registerMetricSet(blockHandler.getAllMetrics)
    metricsSystem.registerSource(shuffleServiceSource)
    metricsSystem.start()

    startWebInterface(metricsSystem, webUi)
  }

  /** Start the external shuffle service */
  def start(): Unit = {
    require(server == null, "Shuffle server already started")
    val authEnabled = securityManager.isAuthenticationEnabled()
    logInfo(s"Starting shuffle service on host $host")
    logInfo(s"Starting shuffle service on port $port (auth enabled = $authEnabled)")
    val bootstraps: Seq[TransportServerBootstrap] =
      if (authEnabled) {
        Seq(new AuthServerBootstrap(transportConf, securityManager))
      } else {
        Nil
      }
    transportContext = new TransportContext(transportConf, blockHandler, true)
    server = transportContext.createServer(host, port, bootstraps.asJava)

    shuffleServiceSource.registerMetricSet(server.getAllMetrics)
    blockHandler.getAllMetrics.getMetrics.put("numRegisteredConnections",
      server.getRegisteredConnections)
    shuffleServiceSource.registerMetricSet(blockHandler.getAllMetrics)
    metricsSystem.registerSource(shuffleServiceSource)
    metricsSystem.start()
  }

  /** Clean up all shuffle files associated with an application that has exited. */
  def applicationRemoved(appId: String): Unit = {
    blockHandler.applicationRemoved(appId, true /* cleanupLocalDirs */)
  }

  /** Clean up all the non-shuffle files associated with an executor that has exited. */
  def executorRemoved(executorId: String, appId: String): Unit = {
    blockHandler.executorRemoved(executorId, appId)
  }

  def stop(webUi: ShuffleServiceWebUI): Unit = {
    if (server != null) {
      server.close()
      server = null
    }
    if (transportContext != null) {
      transportContext.close()
      transportContext = null
    }
    if (webUi != null){
      webUi.stop()
    }
  }

  def stop(): Unit = {
    if (server != null) {
      server.close()
      server = null
    }
    if (transportContext != null) {
      transportContext.close()
      transportContext = null
    }
  }
}
