package org.apache.spark.deploy.shuffleService

import java.io.File
import java.net.BindException
import java.util.concurrent.CountDownLatch
import org.apache.spark.deploy.DeployMessages.{RequestShuffleServiceState, ShuffleServiceStateResponse}
import org.apache.spark.internal.config.SHUFFLE_SERVICE_PORT
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.util.{ShutdownHookManager, SparkUncaughtExceptionHandler, Utils}
import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.shuffleService.ui.ShuffleServiceWebUI
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rpc.{IsolatedRpcEndpoint, RpcAddress, RpcCallContext, RpcEnv}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{DeregisterCherryNode, RegisterNewCherryNode}
import org.apache.spark.serializer.{JavaSerializer, SerializerInstance}

class CherryServiceRunner(
    override val rpcEnv: RpcEnv,
    val args: ExternalShuffleServiceArguments,
    val securityMgr: SecurityManager,
    val conf: SparkConf)
  extends IsolatedRpcEndpoint with Logging { //extends App

  val sparkConf: SparkConf = conf //new SparkConf//(false)
  private[spark] var cherryEndPoint: ShuffleServiceBackend = null
  private val host = args.host
  private val port = args.port

  sparkConf.set("spark.shuffle.service.enabled", "true")
  sparkConf.set("spark.local.dir", System.getProperty("java.io.tmpdir")) // "/tmp" "/tmp/shuffle") //,
  sparkConf.set("spark.shuffle.service.host", host) //"localhost") //"10.0.1.174" "metallica-Inspiron-5567")

  Utils.loadDefaultSparkProperties(sparkConf, null)
  val securityManager: SecurityManager = securityMgr //new SecurityManager(sparkConf)

  private val barrier = new CountDownLatch(1)
  // we override this value since this service is started from the command line
  // and we assume the user really wants it to be running
  sparkConf.set(config.SHUFFLE_SERVICE_ENABLED.key, "true")
  sparkConf.set(config.SHUFFLE_SERVICE_PORT, port)
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

  val closureSerializer = new JavaSerializer(sparkConf)
  val ser: SerializerInstance = closureSerializer.newInstance()

  def shuffleServiceConf: SparkConf = sparkConf.clone().set(SHUFFLE_SERVICE_PORT, port)

  private val server = new CherryShuffleService(sparkConf, securityManager) //(shuffleServiceConf, securityManager)

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RequestShuffleServiceState =>
      context.reply(ShuffleServiceStateResponse(
        host,
        webUiPort,
        server.blockHandler.getAllMetrics.getMetrics.get("blockTransferRateBytes").toString,
        server.blockHandler.getAllMetrics.getMetrics.get("registeredExecutorsSize").toString,
        server.blockHandler.getAllMetrics.getMetrics.get("numActiveConnections").toString,
        server.blockHandler.getAllMetrics.getMetrics.get("numCaughtExceptions").toString))
  }


  /*class RemoteShuffleServiceEndpoint extends IsolatedRpcEndpoint with Logging {

    //override val rpcEnv: RpcEnv = CoarseGrainedShuffleServiceBackend.this.rpcEnv //startRpcEnvAndEndpoint(args, args.host, 7788, conf) //
    override val rpcEnv: RpcEnv = CherryServiceRunner.this.rpcEnv //rpcEnvForDriver //RpcEnv.create("RSS-TEST", host, 7788, conf, securityMgr)

    override def onStart(): Unit = {
      logInfo("Setting up separate RpcEndpoint for communication with Spark Drivers TEST")
    }

    override def receive: PartialFunction[Any, Unit] = {
      case LaunchTask(data) =>
        logInfo("LaunchTask message arrived from Driver.")
        val taskDesc = TaskDescription.decode(data.value)
        logInfo("Got assigned task " + taskDesc.taskId)
      //taskResources(taskDesc.taskId) = taskDesc.resources
      //eecutor.launchTask(this, taskDesc)

      /*case Shutdown =>
        stopping.set(true)
        new Thread("CoarseGrainedExecutorBackend-stop-executor") {
          override def run(): Unit = {
            // executor.stop() will call `SparkEnv.stop()` which waits until RpcEnv stops totally.
            // However, if `executor.stop()` runs in some thread of RpcEnv, RpcEnv won't be able to
            // stop until `executor.stop()` returns, which becomes a dead-lock (See SPARK-14180).
            // Therefore, we put this line in a new thread.
            executor.stop()
          }
        }.start()*/
      case LaunchedExecutor(msg) =>
        logInfo(msg)
      case e =>
        logError(s"Received unexpected message. ${e}")
    }

    override def onStop(): Unit = {
      logInfo("Stopping separate RpcEndpoint for communication with Spark Drivers")
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case LaunchedExecutor(msg) =>
        logInfo(msg)
        context.reply("OK", msg)
      case e =>
        logError(s"Received unexpected message. ${e}")
    }
  }

  val rssEndpoint: RpcEndpointRef = rpcEnv.setupEndpoint("RemoteShuffleServiceTEST", createRSSEndpoint) //CherryServiceRunner.ENDPOINT_NAME, createRSSEndpoint) //new RemoteShuffleServiceEndpoint())
  def createRSSEndpoint: RemoteShuffleServiceEndpoint = new RemoteShuffleServiceEndpoint()
*/
  try {
    createWorkDir()
    //webUi = new ShuffleServiceWebUI(this, workDir, webUiPort)

    //server.start(webUi)
    server.start()
    logInfo("==============================================================")
    logInfo("CHERRY Shuffle Service started!")
    logInfo(s"Host: ${sparkConf.get(config.SHUFFLE_SERVICE_HOST)}, Port: ${shuffleServiceConf.get(config.SHUFFLE_SERVICE_PORT)}") //.key

    // Start Cherry Endpoint if look-ahead caching is enabled
    if (sparkConf.get(config.LOOK_AHEAD_CACHING_ENABLED)){
      //rssEndPoint = new CoarseGrainedShuffleServiceEndpoint(rpcEnv)
      cherryEndPoint = new ShuffleServiceBackend(securityManager, server.blockHandler, conf, args.lookAheadCachingHost)
      cherryEndPoint.startEndpoint()
    }

    // Register in Metadata Service if Distributed Cherry is enabled
    if (sparkConf.get(config.DISTRIBUTED_CHERRY_ENABLED)){
      logInfo("Registering in Metadata Service.")
      val metadataHost = sparkConf.get(config.METADATA_SERVICE_HOST)
      val metadataPort = sparkConf.get(config.METADATA_SERVICE_PORT)
      val metadataEndpoint = rpcEnv.setupEndpointRef(RpcAddress(metadataHost, metadataPort), "MetadataServiceEndpoint")
       if (!metadataEndpoint.askSync[Boolean](RegisterNewCherryNode(JavaUtils.bufferToArray(ser.serialize(sparkConf.get(config.SHUFFLE_SERVICE_HOST), shuffleServiceConf.get(config.SHUFFLE_SERVICE_PORT)))))) {
        throw new SparkException("Metadata Service returned false, expected true for successful registration.")
      }
    }


  } catch {
    case e: BindException => logError(s"$e : Address already in use.")
    case e: Exception => logError(s"An exception occurred: $e")
    case _ => logError("An exception occurred.")
  }
  logDebug("Adding shutdown hook") // force eager creation of logger
  ShutdownHookManager.addShutdownHook { () =>
    logInfo("Shutting down shuffle service.")
    //server.stop(webUi)

    // De-register from Metadata Service if Distributed Cherry is enabled
    if (sparkConf.get(config.DISTRIBUTED_CHERRY_ENABLED)) {
      val metadataHost = sparkConf.get(config.METADATA_SERVICE_HOST)
      val metadataPort = sparkConf.get(config.METADATA_SERVICE_PORT)
      val metadataEndpoint = rpcEnv.setupEndpointRef(RpcAddress(metadataHost, metadataPort), "MetadataServiceEndpoint")
      if (!metadataEndpoint.askSync[Boolean](DeregisterCherryNode(JavaUtils.bufferToArray(ser.serialize(sparkConf.get(config.SHUFFLE_SERVICE_HOST), shuffleServiceConf.get(config.SHUFFLE_SERVICE_PORT)))))) {
        throw new SparkException("Metadata Service returned false, expected true for successful de-registration.")
      }
    }

    server.stop()
    barrier.countDown()
    cherryEndPoint.stopEndpoint()
  }

  // keep running until the process is terminated
  barrier.await()

}


private[deploy] object CherryServiceRunner extends Logging {
  val SYSTEM_NAME = "sparkShuffleService"
  val ENDPOINT_NAME = "RemoteShuffleService"

  def main(argStrings: Array[String]): Unit = {
    Thread.setDefaultUncaughtExceptionHandler(new SparkUncaughtExceptionHandler(
      exitOnUncaughtException = false))
    Utils.initDaemon(log)
    val conf = new SparkConf
    val args = new ExternalShuffleServiceArguments(argStrings, conf)
    val rpcEnv = startRpcEnvAndEndpoint(args, args.host, args.port, conf)
    rpcEnv.awaitTermination()
  }

  def startRpcEnvAndEndpoint(
                              args: ExternalShuffleServiceArguments,
                              host: String,
                              port: Int,
                              conf: SparkConf = new SparkConf): RpcEnv = {

    // The LocalSparkCluster runs multiple local sparkWorkerX RPC Environments
    val systemName = SYSTEM_NAME
    val securityMgr = new SecurityManager(conf)
    val rpcEnv = RpcEnv.create(systemName, host, 0, conf, securityMgr)
    rpcEnv.setupEndpoint("RemoteShuffleServiceTEST", new CherryServiceRunner(rpcEnv, args, securityMgr, conf))
    rpcEnv
  }
}
