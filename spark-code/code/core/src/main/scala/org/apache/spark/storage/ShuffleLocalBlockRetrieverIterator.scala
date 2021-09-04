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

package org.apache.spark.storage

import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.{OneForOneLocalBlockRetriever, RemoteBlockHandler}
import org.apache.spark.storage.ShuffleLocalBlockRetrieverIterator.assertPositiveBlockSize
import org.apache.spark.util.Utils
import java.io.InputStream
import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable.{ArrayBuffer, Queue}

/**
 * An iterator that retrieves multiple blocks.
 */
final class ShuffleLocalBlockRetrieverIterator(
    blocksByAddress: Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])],
    appId: String,
    blockHandler: RemoteBlockHandler)
  extends Iterator[(BlockId, InputStream)] with Logging {

  import ShuffleBlockFetcherIterator._

  /**
   * Queue of fetch requests to issue; we'll pull requests off this gradually to make sure that
   * the number of bytes in flight is limited to maxBytesInFlight.
   */
  private[this] val fetchRequests = new Queue[FetchRequest]

  /**
   * A queue to hold our results. This turns the asynchronous model provided by
   * [[org.apache.spark.network.BlockTransferService]] into a synchronous model (iterator).
   */
  private[this] val results = new LinkedBlockingQueue[FetchResult]

  private[this] val startTimeNs = System.nanoTime()

  initialize()



  private[this] def initialize(): Unit = {

    val blockRequests = findRequiredBlocksToCache()

    /*fetchRequests ++= blockRequests //++= Utils.randomize(blockRequests)

    logTrace(s"Started ${blockRequests.size} local fetches in ${Utils.getUsedTimeNs(startTimeNs)}")
    fetchAvailableRequestsFromQueue(appId)*/

  }

  //////////////////
  //////////////////

  def findRequiredBlocksToCache(): ArrayBuffer[FetchRequest] = {
    val collectedRemoteRequests = new ArrayBuffer[FetchRequest]

    for ((address, blockInfos) <- blocksByAddress) {
      collectFetchRequests(address, blockInfos, collectedRemoteRequests)
    }
    val numTotalBlocks = collectedRemoteRequests.map(_.blocks.size).sum
    logTrace(s"Number of total blocks: $numTotalBlocks")
    collectedRemoteRequests
  }

  def collectFetchRequests(
      address: BlockManagerId,
      blockInfos: Seq[(BlockId, Long, Int)],
      collectedRemoteRequests: ArrayBuffer[FetchRequest]): Unit = {
    val iterator = blockInfos.iterator
    var curBlocks = Seq.empty[FetchBlockInfo]

    while (iterator.hasNext) {
      val (blockId, size, mapIndex) = iterator.next()

      assertPositiveBlockSize(blockId, size)
      curBlocks = curBlocks ++ Seq(FetchBlockInfo(blockId, size, mapIndex))
    }
    // Add in the final request
    if (curBlocks.nonEmpty) {
      logDebug(s"Creating fetch request of ${curBlocks.map(_.size).sum} at $address "
        + s"with ${curBlocks.size} blocks")
      collectedRemoteRequests += FetchRequest(address, curBlocks)

      // Find block locally
      findBlock(FetchRequest(address, curBlocks), appId) //- avoid using queue

      //curBlocks = createFetchRequests(curBlocks, address, collectedRemoteRequests)
    }
  }


  /*def createFetchRequests(
                           curBlocks: Seq[FetchBlockInfo],
                           address: BlockManagerId,
                           collectedRemoteRequests: ArrayBuffer[FetchRequest]): Seq[FetchBlockInfo] = {
    logDebug(s"Creating fetch request of ${curBlocks.map(_.size).sum} at $address "
      + s"with ${curBlocks.size} blocks")
    collectedRemoteRequests += FetchRequest(address, curBlocks)
    curBlocks
  }*/


  def fetchAvailableRequestsFromQueue(appId: String): Unit = {
    // Process any regular local fetch requests if possible.
    while (isFetchQueueNonEmpty(fetchRequests)) {
      val request = fetchRequests.dequeue()
      // Find block locally
      findBlock(request, appId)
    }

    def isFetchQueueNonEmpty(fetchReqQueue: Queue[FetchRequest]): Boolean = {
      fetchReqQueue.nonEmpty
    }
  }


  def findBlock(req: FetchRequest, appId: String): Unit = {
    logDebug("Answering request for %d blocks (%s) from %s".format(
      req.blocks.size, Utils.bytesToString(req.size), req.address.hostPort))

    val blockIds = req.blocks.map(_.blockId.toString)
    val address = req.address
    logTrace(s"---- Req Blocks: ${req.blocks}")

    logDebug(s"Locally retrieving blocks pushed to ${address.host}:${address.port} from (executor id ${address.executorId})")
    try {
      val blockFetchStarter: Unit = new OneForOneLocalBlockRetriever(address.executorId, appId, blockIds.toArray,blockHandler).start()
    } catch {
      case e: Exception =>
        logError("Exception while beginning local retrieval of blocks in cache", e)
    }

  }


  /////////////////
  /////////////////
  override def hasNext: Boolean = ???

  override def next(): (BlockId, InputStream) = ???
}


object ShuffleLocalBlockRetrieverIterator {
  /////////////////////////////////////
  ////////////////////////////////////
  def assertPositiveBlockSize(blockId: BlockId, blockSize: Long): Unit = { //private
    if (blockSize < 0) {
      throw BlockException(blockId, "Negative block size " + blockSize)
    } else if (blockSize == 0) {
      throw BlockException(blockId, "Zero-sized blocks should be excluded.")
    }
  }
  private[spark] case class FetchBlockInfo(
                                            blockId: BlockId,
                                            size: Long,
                                            mapIndex: Int)
  private[spark] case class SuccessFetchResult(
                                                blockId: BlockId,
                                                mapIndex: Int,
                                                address: BlockManagerId,
                                                size: Long,
                                                buf: ManagedBuffer,
                                                isNetworkReqDone: Boolean) extends FetchResult {
    require(buf != null)
    require(size >= 0)
  }

  private[spark] sealed trait FetchResult {
    val blockId: BlockId
    val address: BlockManagerId
  }

  private[spark] case class FailureFetchResult(
                                                blockId: BlockId,
                                                mapIndex: Int,
                                                address: BlockManagerId,
                                                e: Throwable)
    extends FetchResult

  case class FetchRequest(address: BlockManagerId, blocks: Seq[FetchBlockInfo]) {
    val size = blocks.map(_.size).sum
  }
}
