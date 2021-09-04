package org.apache.spark.deploy.metadataService

import java.io.File
import java.net.BindException
import java.util.concurrent.CountDownLatch
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.util.{ShutdownHookManager, SparkUncaughtExceptionHandler, Utils}
import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.rpc.{RpcAddress, RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{DeregisterCherryNode, DeregisterExec, GetCherryNodesInfo, NewCherryNodeRegistered, RegisterNewCherryNode}
import org.apache.spark.serializer.{JavaSerializer, SerializerInstance}

import java.nio.ByteBuffer
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.collection.mutable

class MetadataService(
   //override val rpcEnv: RpcEnv,
   val args: MetadataServiceArguments,
   val securityMgr: SecurityManager,
   val conf: SparkConf)
  extends RpcEndpoint with Logging {

  val sparkConf: SparkConf = conf
  private val host = args.host
  private val port = args.port

  Utils.loadDefaultSparkProperties(sparkConf, null)
  val securityManager: SecurityManager = securityMgr //new SecurityManager(sparkConf)

  private val barrier = new CountDownLatch(1)
  // we override this value since this service is started from the command line
  // and we assume the user really wants it to be running
  sparkConf.set("spark.distributed.cherry.enabled", "true")
  sparkConf.set("spark.local.dir", System.getProperty("java.io.tmpdir"))
  sparkConf.set(config.METADATA_SERVICE_HOST, host)
  sparkConf.set(config.METADATA_SERVICE_PORT, port)

  private val sparkHome = new File(sys.env.getOrElse("SPARK_HOME", "."))
  var workDir: File = null
  var workDirPath: String = null
  private def createWorkDir(): Unit = {
    workDir = Option(workDirPath).map(new File(_)).getOrElse(new File(sparkHome, "work"))
    if (!Utils.createDirectory(workDir)) {
      System.exit(1)
    }
  }

  private val endpoint = new AtomicReference[RpcEndpointRef]
  override val rpcEnv: RpcEnv = RpcEnv.create(MetadataService.SYSTEM_NAME, host, port, conf, securityMgr)


  val closureSerializer = new JavaSerializer(conf)
  val ser: SerializerInstance = closureSerializer.newInstance()
  private val cherryNodeInfoMap =  new mutable.HashMap[Int, Array[_]]//Array[Array[_]] // Array = [ip, port]
  private lazy val cherryNodeId = new AtomicInteger(0)
  private[metadataService] def getNextCherryId: Int = cherryNodeId.getAndIncrement()
  private val sparkExecutorInfoMap =  new mutable.HashMap[Int, Array[_]]
  private lazy val executorNodeId = new AtomicInteger(0)
  private[metadataService] def getNextExecutorId: Int = executorNodeId.getAndIncrement()

  class MetadataServiceEndpoint extends RpcEndpoint with Logging {

    override val rpcEnv: RpcEnv = MetadataServiceEndpoint.this.rpcEnv
    override def onStart(): Unit = {
      logInfo(s"Setting up RpcEndpoint for communication with Cherry Shuffle Service nodes and Spark Executors on ${conf.get(config.METADATA_SERVICE_HOST)} : ${conf.get(config.METADATA_SERVICE_PORT)}")
    }

    override def receive: PartialFunction[Any, Unit] = {

      case e =>
        logError(s"Received unexpected message. ${e}")
    }

    override def onStop(): Unit = {
      logInfo("Stopping separate RpcEndpoint for communication with Spark Drivers")
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

      case RegisterNewCherryNode(msg) =>
        context.reply(true)

        val (cherryIp, cherryPort) = ser.deserialize[(String, Int)](
          ByteBuffer.wrap(msg), Thread.currentThread.getContextClassLoader)
        logInfo(s"RegisterNewCherryNode message arrived. IP: ${cherryIp}, PORT: ${cherryPort}")
        cherryNodeInfoMap(getNextCherryId) = Array(cherryIp, cherryPort)
        cherryNodeInfoMap.foreach { case (key, values) => logInfo("key " + key + " -> " + values.mkString(" : "))
        }

        if (sparkExecutorInfoMap.nonEmpty) {
          logInfo("There are available Spark executors that need to be informed for new Cherry node additions.")
          for ((_, values) <- sparkExecutorInfoMap) {
            /*sparkExecutorInfoMap.foreach {
              case (key, values) =>*/
            val executorHost = values.apply(0).asInstanceOf[String]
            val executorPort = values.apply(1).asInstanceOf[Int]
            logInfo(s"Executor to inform: ${executorHost} : ${executorPort}.")
            try{
              val executorEndpoint = rpcEnv.setupEndpointRef(RpcAddress(executorHost, executorPort), "Executor")
              executorEndpoint.askSync[Boolean](NewCherryNodeRegistered(msg)) // send instead of askSync? is ack needed?
            }
            catch{
              case e: NullPointerException => logError(s"$e : Cannot send message to executor. It is possibly stopped.")
              case e: Exception => logError(s"An exception occurred: $e")
              case _ => logError("An exception occurred.")
            }
          }
          logInfo("Inform process finished.")
        }

      case GetCherryNodesInfo() =>
        val hostPort = context.senderAddress.hostPort
        //if (!context.senderAddress.host.equals("spark-driver")) {
        sparkExecutorInfoMap(getNextExecutorId) = Array(context.senderAddress.host, context.senderAddress.port)
        //}
        logInfo(s"GetCherryNodesInfo message arrived from ${hostPort}.")
        context.reply(JavaUtils.bufferToArray(ser.serialize(cherryNodeInfoMap)))

      case DeregisterCherryNode(msg) =>
        context.reply(true)

        val (cherryIp, cherryPort) = ser.deserialize[(String, Int)](
          ByteBuffer.wrap(msg), Thread.currentThread.getContextClassLoader)
        logInfo(s"DeregisterCherryNode message arrived. IP: ${cherryIp}, PORT: ${cherryPort}")
        cherryNodeInfoMap(getNextCherryId) = Array(cherryIp, cherryPort)
        cherryNodeInfoMap.foreach { case (key, values) =>
          if (values sameElements Array(cherryIp, cherryPort)){
            cherryNodeInfoMap -= key //cherryNodeInfoMap.remove(key)
            logTrace("Removed deregistered Cherry node from list.")
            cherryNodeInfoMap.foreach { case (key, values) => logInfo("key " + key + " -> " + values.mkString(" : "))
            }
          }
        }

      case DeregisterExec() =>
        context.reply(true)
        val execHost = context.senderAddress.host
        val execPort = context.senderAddress.port
        logInfo(s"DeregisterExec message arrived. IP: ${execHost}, PORT: ${execPort}")
        sparkExecutorInfoMap.foreach { case (key, values) =>
          if (values sameElements Array(execHost, execPort)){
            sparkExecutorInfoMap -= key
            logInfo("Removed deregistered Executor node from list.")
            sparkExecutorInfoMap.foreach { case (key, values) => logInfo("key " + key + " -> " + values.mkString(" : "))
            }
          }
        }

      case e =>
        logError(s"Received unexpected message. ${e}")
    }

  }



  try {
    createWorkDir()

    logInfo("==============================================================")
    logInfo("METADATA Service started!")
    logInfo(s"Host: ${sparkConf.get(config.METADATA_SERVICE_HOST)}, Port: ${sparkConf.get(config.METADATA_SERVICE_PORT)}")
    startEndpoint()
  } catch {
    case e: BindException => logError(s"$e : Address already in use.")
    case e: Exception => logError(s"An exception occurred: $e")
    case _ => logError("An exception occurred.")
  }
  logInfo("Adding shutdown hook") // force eager creation of logger
  ShutdownHookManager.addShutdownHook { () =>
    logInfo("Shutting down metadata service.")
    barrier.countDown()
  }
  // keep running until the process is terminated
  barrier.await()

  def startEndpoint(): Unit = {
    logInfo("Doing Setup of Endpoint...")
    // Just launch an rpcEndpoint; it will call back into the listener.
    endpoint.set(rpcEnv.setupEndpoint("MetadataServiceEndpoint", new MetadataServiceEndpoint()))
    logInfo("Done.")
  }

  def stopEndpoint(): Unit = {
    if (endpoint.get != null) {
      try {
        endpoint.set(null)
      } catch {
        case e: Exception =>
          logInfo("Stop request failed; it may already be shut down.")
      }

    }
  }

}


private[deploy] object MetadataService extends Logging {
  val SYSTEM_NAME = "sparkMetadataService"
  val ENDPOINT_NAME = "MetadataService"

  def main(argStrings: Array[String]): Unit = {
    Thread.setDefaultUncaughtExceptionHandler(new SparkUncaughtExceptionHandler(
      exitOnUncaughtException = false))
    Utils.initDaemon(log)
    val conf = new SparkConf
    val args = new MetadataServiceArguments(argStrings, conf)
    //val rpcEnv = startRpcEnvAndEndpoint(args, args.host, args.port, conf)

    //val systemName = SYSTEM_NAME
    val securityMgr = new SecurityManager(conf)
    //val rpcEnv = RpcEnv.create(systemName, args.host, args.port, conf, securityMgr)

    val metadataServiceEndpoint = new MetadataService(args, securityMgr, conf)
    //metadataServiceEndpoint.startEndpoint()

    //rpcEnv.awaitTermination()
  }

  /*def startRpcEnvAndEndpoint(
        args: MetadataServiceArguments,
        host: String,
        port: Int,
        conf: SparkConf = new SparkConf): RpcEnv = {

    val systemName = SYSTEM_NAME
    val securityMgr = new SecurityManager(conf)
    val rpcEnv = RpcEnv.create(systemName, host, port, conf, securityMgr)
    rpcEnv.setupEndpoint("MetadataServiceEndpoint", new MetadataService(rpcEnv, args, securityMgr, conf))
    rpcEnv
  }*/
}