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


import org.apache.spark.internal.config.{ConfigEntry, DISTRIBUTED_CHERRY_ENABLED, LOOK_AHEAD_CACHING_ENABLED}
import org.apache.spark.{MapOutputTrackerShuffleService, Partition, SecurityManager, ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.network.shuffle.RemoteBlockHandler
import org.apache.spark.rdd.{InputFileBlockHolder, RDD, UnionPartition, ZippedPartitionsPartition}
import org.apache.spark.rpc.{RpcEnv, _}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.scheduler.{Task, TaskDescription}
import org.apache.spark.util._
import org.apache.spark.serializer.{JavaSerializer, SerializerInstance}
import org.apache.spark.storage.ShuffleLocalBlockRetrieverIterator

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable


private[spark] class ShuffleServiceBackend(
   val securityMgr: SecurityManager,
   val blockHandler: RemoteBlockHandler,
   val conf: SparkConf,
   val cachingEndpointHost: String)
  extends RpcEndpoint with Logging {

  //private val host = rpcEnv.address.host
  //private val port = rpcEnv.address.port
  //Utils.checkHost(host)
  //assert (port > 0)
  private val endpoint = new AtomicReference[RpcEndpointRef]

  private var stageBroadcastBlockMap = new mutable.HashMap[Int, Array[_]]

  /**
   * The [[RpcEnv]] that this [[RpcEndpoint]] is registered to.
   */
  override val rpcEnv: RpcEnv = RpcEnv.create(CherryServiceRunner.SYSTEM_NAME, cachingEndpointHost, conf.get(config.LOOK_AHEAD_CACHING_PORT), conf, securityMgr)

  val closureSerializer = new JavaSerializer(conf)
  val ser: SerializerInstance = closureSerializer.newInstance()
  val mapOutputTracker = new MapOutputTrackerShuffleService(conf, securityMgr)
  //---mapOutputTracker.trackerEndpoint = RpcUtils.makeDriverRef(MapOutputTracker.ENDPOINT_NAME, conf, rpcEnv)
  class CoarseGrainedShuffleServiceEndpoint extends RpcEndpoint with Logging {

    override val rpcEnv: RpcEnv = CoarseGrainedShuffleServiceEndpoint.this.rpcEnv //startRpcEnvAndEndpoint(args, args.host, 7788, conf) //

    override def onStart(): Unit = {
      logInfo(s"Setting up separate RpcEndpoint for communication with Spark Drivers on ${cachingEndpointHost} : ${ conf.get(config.LOOK_AHEAD_CACHING_PORT)}")
    }

    override def receive: PartialFunction[Any, Unit] = {

      case BroadCastTaskBinary(taskBinary) =>
        logInfo("BroadCastTaskBinary message arrived from Driver: " + ByteBuffer.wrap(taskBinary))

      /*case SerMapStatusForShuffleMapStage(data, mapStatus) =>
        //val (stageId, stageType, stageRDD, stageShuffleDep, depShuffleId) = ser.deserialize[(Int, String, RDD[_], ShuffleDependency[_, _, _], Int)](
        val (stageId, stageType, stageRDDNoOfPartitions, depShuffleId) = ser.deserialize[(Int, String, Int, Int)](
          ByteBuffer.wrap(data), Thread.currentThread.getContextClassLoader)
        val serMapStatus = mapStatus
        logInfo(s"BroadCastTaskBinaryForShuffleMapStage message arrived from Driver with StageId: $stageId")
        logTrace(s"Other values: $stageRDDNoOfPartitions, $stageType, $depShuffleId, $serMapStatus")

        // Check if stageId already exists in order to remove values from the stageBroadcastBlockMap and serMapStatuses
        if (stageBroadcastBlockMap.contains(stageId)){
          stageBroadcastBlockMap = new mutable.HashMap[Int, Array[_]]
          //mapOutputTracker.serMapStatuses = new mutable.HashMap[Int, Array[Byte]]
        }
       // val arr = Array(stageType, stageRDD, stageShuffleDep)
        stageBroadcastBlockMap(stageId) = Array(stageType, stageRDDNoOfPartitions, depShuffleId)
        mapOutputTracker.serMapStatuses.put(depShuffleId, serMapStatus)
      */
      case SerMapStatusForResultStage(data, mapStatus) =>
        //val (stageId, stageType, stageRDD, stageFunc, depShuffleId) = ser.deserialize[(Int, String, RDD[_], (TaskContext, Iterator[_]) => _, Int)](
        val (stageId, stageType, stageRDDNoOfPartitions, depShuffleId) = ser.deserialize[(Int, String, Int, Int)](
          ByteBuffer.wrap(data), Thread.currentThread.getContextClassLoader)
        val serMapStatus = mapStatus
        logInfo(s"BroadCastTaskBinaryForResultStage message arrived from Driver with StageId: $stageId")
        logTrace(s"Other values: $stageRDDNoOfPartitions, $stageType, $depShuffleId, $serMapStatus")

        // Check if stageId already exists in order to remove values from the stageBroadcastBlockMap and serMapStatuses
        if (stageBroadcastBlockMap.contains(stageId)){
          stageBroadcastBlockMap = new mutable.HashMap[Int, Array[_]]
          //mapOutputTracker.serMapStatuses = new mutable.HashMap[Int, Array[Byte]]
        }
        // val arr = Array(stageType, stageRDD, stageShuffleDep)
        stageBroadcastBlockMap(stageId) = Array(stageType, stageRDDNoOfPartitions, depShuffleId)
        mapOutputTracker.serMapStatuses.put(depShuffleId, serMapStatus)

      case SerMapStatusForShuffleMapStage(data, mapStatus) =>
        val (stageId, stageType, stageRDDNoOfPartitions, depShuffleId) = ser.deserialize[(Int, String, Int, Int)](
          ByteBuffer.wrap(data), Thread.currentThread.getContextClassLoader)
        val serMapStatus = mapStatus
        logInfo(s"SerMapStatusForShuffleMapStage message arrived from Driver with StageId: $stageId")
        logTrace(s"Other values: $stageRDDNoOfPartitions, $stageType, $depShuffleId, $serMapStatus")

        // Check if stageId already exists in order to remove values from the stageBroadcastBlockMap and serMapStatuses
        if (stageBroadcastBlockMap.contains(stageId)){
          stageBroadcastBlockMap = new mutable.HashMap[Int, Array[_]]
          //mapOutputTracker.serMapStatuses = new mutable.HashMap[Int, Array[Byte]]
        }
        stageBroadcastBlockMap(stageId) = Array(stageType, stageRDDNoOfPartitions, depShuffleId)
        mapOutputTracker.serMapStatuses.put(depShuffleId, serMapStatus)

      case SerMapStatusForShuffleMapStageUnion(data, mapStatus) =>
        val (stageId, stageType, stageRDDNoOfPartitions, depShuffleIdArray, partitions) = ser.deserialize[(Int, String, Int, Array[Int], Array[Partition])](
          ByteBuffer.wrap(data), Thread.currentThread.getContextClassLoader)
        val serMapStatusArray = mapStatus
        logInfo(s"SerMapStatusForShuffleMapStageUnion message arrived from Driver with StageId: $stageId")
        logTrace(s"Other values: $stageRDDNoOfPartitions, ${partitions.mkString(", ")}, $stageType, ${depShuffleIdArray.mkString(", ")}, $serMapStatusArray")

        // Check if stageId already exists in order to remove values from the stageBroadcastBlockMap and serMapStatuses
        if (stageBroadcastBlockMap.contains(stageId)){
          stageBroadcastBlockMap = new mutable.HashMap[Int, Array[_]]
          //mapOutputTracker.serMapStatuses = new mutable.HashMap[Int, Array[Byte]]
        }
        stageBroadcastBlockMap(stageId) = Array(stageType, stageRDDNoOfPartitions, depShuffleIdArray, partitions)

        var counter = 0
        depShuffleIdArray.foreach({depShuffleId =>
          mapOutputTracker.serMapStatuses.put(depShuffleId, serMapStatusArray.apply(counter).asInstanceOf[Array[Byte]])
          counter += 1
        })

      /*case LaunchTask(data) =>
        val taskDesc = TaskDescription.decode(data.value)
        logInfo("LaunchTask message arrived from Driver. Got information for assigned task: " + taskDesc.taskId)
        processTaskDescription(taskDesc)*/

      case LaunchTaskForCherry(appId, partitionId, stageId) => //(data, stageId) =>
        //val taskDesc = TaskDescription.decode(data.value)
        logDebug(s"LaunchTask message arrived from Driver. Got information for assigned task: ${partitionId}, ${stageId}")
        processTaskDescription(appId, partitionId, stageId)

      /*case ClearCache() =>
        if (conf.get(LOOK_AHEAD_CACHING_ENABLED)){
          if (blockHandler.getBlockResolver.dataCache != null) {
            blockHandler.getBlockResolver.dataCache.clear()
          }
          if (blockHandler.getBlockResolver.shuffleDataCache != null) {
            blockHandler.getBlockResolver.shuffleDataCache.invalidateAll()
          }
        }*/

      case e =>
        logError(s"Received unexpected message. ${e}")
    }

    override def onStop(): Unit = {
      logInfo("Stopping separate RpcEndpoint for communication with Spark Drivers")
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case e =>
        logError(s"Received unexpected message. ${e}")
    }
  }

  def startEndpoint(): Unit = {
    logInfo("Doing Setup of Endpoint...")
    // Just launch an rpcEndpoint; it will call back into the listener.
    endpoint.set(rpcEnv.setupEndpoint("RemoteShuffleServiceEndpoint", new CoarseGrainedShuffleServiceEndpoint()))
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



  def instantiateClass[T](className: String): T = {
    val cls = Utils.classForName(className)
    // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
    // SparkConf, then one taking no arguments
    try {
      cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
        .newInstance(conf, java.lang.Boolean.valueOf(false))
        .asInstanceOf[T]
    } catch {
      case _: NoSuchMethodException =>
        try {
          cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
        } catch {
          case _: NoSuchMethodException =>
            cls.getConstructor().newInstance().asInstanceOf[T]
        }
    }
  }
  // Create an instance of the class named by the given SparkConf property
  // if the property is not set, possibly initializing it with our conf
  def instantiateClassFromConf[T](propertyName: ConfigEntry[String]): T = {
    instantiateClass[T](conf.get(propertyName))
  }

 // def processTaskDescription(taskDescription: TaskDescription): Unit = {
  def processTaskDescription(appId: String, partitionId: Int, stageId: Int): Unit = {
    //val taskId = taskDescription.taskId
    //val taskName = taskDescription.name
    //logDebug(s"$taskName")

    ////1st solution to get task
    //val task = taskDescription.serializedTask.asInstanceOf[Task]
    ////2nd solution to get task
    //taskDescription.executorId
    //val ser = Serializer.newInstance()
    //val task = ser.deserialize[Task[Any]](
    //taskDescription.serializedTask, Thread.currentThread.getContextClassLoader)
    //val task = Utils.deserialize[Task[Any]](
    //  taskDescription.serializedTask.array(), Thread.currentThread.getContextClassLoader)
    //val stageId = task.stageId
    // if stageId=0 no shuffle data exist yet
    //if (stageId > 0){
    //InputFileBlockHolder.initialize()
    //logInfo("StageId : "+stageId)
    //logInfo("appId : "+task.appId)
    //logInfo(taskDescription.executorId)
    //}
    //logDebug(s"Using serializer: ${serializer.getClass}
    /*stageBroadcastBlockMap.foreach { case (key, values) => logTrace("key " + key + " : " + values.mkString(" - "))
    }*/
    //val broadcastVal = stageBroadcastBlockMap.get(1) //.asInstanceOf[Array[_]]  //get(stageId).asInstanceOf[Array[_]]
    val broadcastVal = stageBroadcastBlockMap.get(stageId)

    broadcastVal match {
      case Some(broadcastVal) =>
        // Get stage type
        val stageType = broadcastVal.apply(0).asInstanceOf[String]

        if (stageType.equals("ResultStage")){
          logTrace(s"Stage Type: $stageType")

          //val stageRDDNoOfPartitions = broadcastVal.apply(1).asInstanceOf[Int]

          //val func = broadcastVal.apply(2).asInstanceOf[(TaskContext, Iterator[_]) => _]
          //val task = ser.deserialize[Task[Any]](taskDescription.serializedTask, Thread.currentThread.getContextClassLoader)

          //val context = task.context
          //val appId = taskDescription.appId.orNull
          //logTrace(s"Number of partitions: $stageRDDNoOfPartitions")
          logTrace(s"PartitionId of this task: ${partitionId}")
          //taskDescription.serializedTask.

          //val stageId = task.stageId
          //logInfo("Task StageId : "+stageId)

          val depShuffleId = broadcastVal.apply(2).asInstanceOf[Int]
          logTrace(s"This stage is dependent on the shuffleId: $depShuffleId")

          val serializedMapStatus = mapOutputTracker.serMapStatuses.get(depShuffleId).head //broadcastVal.apply(3).asInstanceOf[Array[Byte]] //previousSerializedMapStatus//

          //logDebug(s"Current serializedMapStatus length: ${serializedMapStatus.length}")
          logDebug(s"Current serializedMapStatus: ${serializedMapStatus}")

          /* val dep = rdd.dependencies.head //.asInstanceOf[ShuffleDependency[_, _, _]]
         var shuffleId = 0
         dep match {
           case value: ShuffleDependency[_, _, _] =>
             shuffleId = value.shuffleId
           case _ =>
         }
         logDebug(s"Current shuffleId: $shuffleId")*/

          val blocksByAddress = mapOutputTracker.getMapSizesByExecutorId( //SparkEnv.mapOutputTracker.getMapSizesByExecutorId(
            broadcastVal.apply(2).asInstanceOf[Int], serializedMapStatus, 0, Int.MaxValue, partitionId, partitionId + 1)

          if (conf.get(DISTRIBUTED_CHERRY_ENABLED)){
            // Filter addresses that are not the same with this Cherry node (useful for distributed Cherry system)
            val filteredBlocksByAddress = blocksByAddress.filter( blockByAddress => {blockByAddress._1.host.equals(InetAddress.getLocalHost.getHostAddress)})
            val iter = new ShuffleLocalBlockRetrieverIterator(filteredBlocksByAddress, appId, blockHandler)
          }
          else{
            val iter = new ShuffleLocalBlockRetrieverIterator(blocksByAddress, appId, blockHandler)
          }
        }

        else if (stageType.equals("ShuffleMapStageUnion")){
          logTrace(s"Stage Type: $stageType")

          //val stageRDDNoOfPartitions = broadcastVal.apply(1).asInstanceOf[Int]

          //val task = ser.deserialize[Task[Any]](taskDescription.serializedTask, Thread.currentThread.getContextClassLoader)

          //val appId = taskDescription.appId.orNull
          //logTrace(s"Number of partitions: $stageRDDNoOfPartitions")
          logTrace(s"PartitionId of this task: ${partitionId}")

          //val stageId = task.stageId
          //val dep = broadcastVal.apply(2).asInstanceOf[ShuffleDependency[_, _, _]]
          //

          val depShuffleIdArray = broadcastVal.apply(2).asInstanceOf[Array[Int]]
          logTrace(s"This stage is dependent on the shuffleId(s): ${depShuffleIdArray.mkString(" ")}")

          val partitions = broadcastVal.apply(3).asInstanceOf[Array[Partition]]
          val taskPartId = partitionId
          val partition = partitions(taskPartId)
          logDebug(s"taskPartId: $taskPartId, partition: $partition")

          // ---
       //-   partition match {
            // ---
            // first case: UnionPartition
            // ---
       //-     case partition: UnionPartition[_] =>
              val part = partition.asInstanceOf[UnionPartition[_]]
              logDebug(s"HMMM UnionPartition -$part, ${part.parentPartition}, ${part.parentRddIndex}")
              //val rdd = broadcastVal.apply(4).asInstanceOf[RDD[_]]
              //rdd.dependencies(part.parentRddIndex).rdd.iterator(part.parentPartition, context)

              depShuffleIdArray.foreach({depShuffleId =>
                val serializedMapStatus = mapOutputTracker.serMapStatuses.get(depShuffleId).head

                logDebug(s"Current serializedMapStatus length: ${serializedMapStatus.length}")
                val blocksByAddress = mapOutputTracker.getMapSizesByExecutorId(
                  depShuffleId, serializedMapStatus, 0, Int.MaxValue, part.parentPartition.index, part.parentPartition.index + 1)
                // Check if distributed mode is used and then filter
                if (conf.get(DISTRIBUTED_CHERRY_ENABLED)){
                  // Filter addresses that are not the same with this Cherry node (useful for distributed Cherry system)
                  val filteredBlocksByAddress = blocksByAddress.filter( blockByAddress => {blockByAddress._1.host.equals(InetAddress.getLocalHost.getHostAddress)})
                  val iter = new ShuffleLocalBlockRetrieverIterator(filteredBlocksByAddress, appId, blockHandler)
                }
                else{
                  val iter = new ShuffleLocalBlockRetrieverIterator(blocksByAddress, appId, blockHandler)
                }
              })

            // ---
            // second case: ZippedPartitionsPartition
            // ---
        /*    case partition: ZippedPartitionsPartition =>
              val part = partition.asInstanceOf[ZippedPartitionsPartition].partitions
              logDebug(s"HMMM ZippedPartitionsPartition -$part - ${part.size}")

              depShuffleIdArray.foreach({depShuffleId =>

                part.foreach({ partitionSelected =>
                  val serializedMapStatus = mapOutputTracker.serMapStatuses.get(depShuffleId).head

                  logDebug(s"Current serializedMapStatus length: ${serializedMapStatus.length}")
                  val blocksByAddress = mapOutputTracker.getMapSizesByExecutorId(
                    depShuffleId, serializedMapStatus, 0, Int.MaxValue, partitionSelected.index, partitionSelected.index + 1)
                  if (conf.get(DISTRIBUTED_CHERRY_ENABLED)){
                    // Filter addresses that are not the same with this Cherry node (useful for distributed Cherry system)
                    val filteredBlocksByAddress = blocksByAddress.filter( blockByAddress => {blockByAddress._1.host.equals(InetAddress.getLocalHost.getHostAddress)})
                    val iter = new ShuffleLocalBlockRetrieverIterator(filteredBlocksByAddress, appId, blockHandler)
                  }
                  else{
                    val iter = new ShuffleLocalBlockRetrieverIterator(blocksByAddress, appId, blockHandler)
                  }
                })

              })
        */
            // ---
            // unknown case
            // ---
       //-     case _ => logWarning(s"Unknown partition: ${partition.getClass}")
       //-  }
        }

        else if (stageType.equals("ShuffleMapStage")) {
          logTrace(s"Stage Type: $stageType")

          //val stageRDDNoOfPartitions = broadcastVal.apply(1).asInstanceOf[Int]

          //val task = ser.deserialize[Task[Any]](taskDescription.serializedTask, Thread.currentThread.getContextClassLoader)

          //val appId = taskDescription.appId.orNull
          //logTrace(s"Number of partitions: $stageRDDNoOfPartitions")
          logTrace(s"PartitionId of this task: ${partitionId}")

          val depShuffleId = broadcastVal.apply(2).asInstanceOf[Int] //1 //test //dep.shuffleHandle.shuffleId //previousShuffleId //
          logDebug(s"This stage is dependent on the shuffleId: $depShuffleId")

          val serializedMapStatus = mapOutputTracker.serMapStatuses.get(depShuffleId).head //testSerMapStatuses.get(0) // //broadcastVal.apply(3).asInstanceOf[Array[Byte]] //previousSerializedMapStatus//

          logDebug(s"Current serializedMapStatus length: ${serializedMapStatus.length}")
          val blocksByAddress = mapOutputTracker.getMapSizesByExecutorId(
            depShuffleId, serializedMapStatus, 0, Int.MaxValue, partitionId, partitionId + 1)
          // Check if distributed mode is used and then filter
          if (conf.get(DISTRIBUTED_CHERRY_ENABLED)){
            // Filter addresses that are not the same with this Cherry node (useful for distributed Cherry system)
            val filteredBlocksByAddress = blocksByAddress.filter( blockByAddress => {blockByAddress._1.host.equals(InetAddress.getLocalHost.getHostAddress)})
            val iter = new ShuffleLocalBlockRetrieverIterator(filteredBlocksByAddress, appId, blockHandler)
          }
          else{
            val iter = new ShuffleLocalBlockRetrieverIterator(blocksByAddress, appId, blockHandler)
          }
        }
        else{
          logTrace(s"Unknown stage type.")
        }
      case None =>
        logInfo("Broadcast value has not arrived yet!")
      }

  }

}

