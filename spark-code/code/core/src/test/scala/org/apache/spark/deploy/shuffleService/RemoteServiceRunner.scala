package org.apache.spark.deploy.shuffleService

import java.io.File
import java.net.BindException
import java.util.concurrent.CountDownLatch
import org.apache.spark.deploy.DeployMessages.{RequestShuffleServiceState, ShuffleServiceStateResponse}
import org.apache.spark.deploy.shuffleService.ui.ShuffleServiceWebUI
import org.apache.spark.internal.config.SHUFFLE_SERVICE_PORT
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rpc.{RpcAddress, RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RegisterNewCherryNode
import org.apache.spark.serializer.{JavaSerializer, SerializerInstance}
import org.apache.spark.util.{ShutdownHookManager, Utils}
import org.apache.spark.{SecurityManager, SparkConf}

class CherryServiceRunner(
    override val rpcEnv: RpcEnv)
  extends ThreadSafeRpcEndpoint with Logging { //extends App

  val sparkConf = new SparkConf(false)
  sparkConf.set("spark.shuffle.service.enabled", "true")
  sparkConf.set("spark.local.dir", System.getProperty("java.io.tmpdir")) // "/tmp" "/tmp/shuffle") //,
  sparkConf.set("spark.shuffle.service.host", sparkConf.get(config.SHUFFLE_SERVICE_HOST)) //"localhost") //"10.0.1.174" "metallica-Inspiron-5567")

  Utils.loadDefaultSparkProperties(sparkConf, null)
  val securityManager = new SecurityManager(sparkConf)

  private val barrier = new CountDownLatch(1)
  // we override this value since this service is started from the command line
  // and we assume the user really wants it to be running
  sparkConf.set(config.SHUFFLE_SERVICE_ENABLED.key, "true")
  sparkConf.set(config.SHUFFLE_SERVICE_PORT, 7777)
  //sparkConf.set(config.EVENT_LOG_ENABLED.key, "true")
  //sparkConf.set(config.EVENT_LOG_DIR.key, "./log-output")

  private val sparkHome = new File(sys.env.getOrElse("SPARK_HOME", "."))
  var workDir: File = null
  var workDirPath: String = null
  private def createWorkDir(): Unit = {
    workDir = Option(workDirPath).map(new File(_)).getOrElse(new File(sparkHome, "work"))
    if (!Utils.createDirectory(workDir)) {
      System.exit(1)
    }
  }

  var webUi: ShuffleServiceWebUI = null
  private val webUiPort = 8080
  // = new ShuffleServiceWebUI(this, webUiPort)

  val closureSerializer = new JavaSerializer(sparkConf)
  val ser: SerializerInstance = closureSerializer.newInstance()


  def shuffleServiceConf: SparkConf = sparkConf.clone().set(SHUFFLE_SERVICE_PORT, 7777)

  private val server = new CherryShuffleService(shuffleServiceConf, securityManager) //,false, 0)

  private val host = rpcEnv.address.host
  private val port = rpcEnv.address.port
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RequestShuffleServiceState =>
      context.reply(ShuffleServiceStateResponse(
        host,
        port,
        server.blockHandler.getAllMetrics.getMetrics.get("blockTransferRateBytes").toString,
        server.blockHandler.getAllMetrics.getMetrics.get("registeredExecutorsSize").toString,
        server.blockHandler.getAllMetrics.getMetrics.get("numActiveConnections").toString,
        server.blockHandler.getAllMetrics.getMetrics.get("numCaughtExceptions").toString))
  }


  try {
    createWorkDir()

    server.start()
    logInfo("==============================================================")
    logInfo("Remote External Shuffle Service started!")
    logInfo(s"Host: ${sparkConf.get(config.SHUFFLE_SERVICE_HOST)}, Port: ${shuffleServiceConf.get(config.SHUFFLE_SERVICE_PORT)}") //.key

  } catch {
    case e: BindException => logError(s"$e : Address already in use.")
    case _ => logError("An exception occurred.")
  }
  logDebug("Adding shutdown hook") // force eager creation of logger
  ShutdownHookManager.addShutdownHook { () =>
    logInfo("Shutting down shuffle service.")
    server.stop(webUi)
    barrier.countDown()
  }

  // keep running until the process is terminated
  barrier.await()

}
