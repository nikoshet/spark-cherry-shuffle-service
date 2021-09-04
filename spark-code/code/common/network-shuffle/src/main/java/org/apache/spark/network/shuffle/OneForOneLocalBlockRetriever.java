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

package org.apache.spark.network.shuffle;

import java.util.ArrayList;
import java.util.HashMap;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.network.shuffle.protocol.FetchShuffleBlocks;

/**
 * Simple wrapper that interprets each chunk as a whole block, and
 * invokes the LocalBlockRetrievingListener appropriately.
 */
public class OneForOneLocalBlockRetriever {
  private static final Logger logger = LoggerFactory.getLogger(OneForOneLocalBlockRetriever.class);

  private final FetchShuffleBlocks message;
  private final String[] blockIds;
  private final String appId;
  private final String execId;
  private final RemoteBlockHandler blockHandler;

  public OneForOneLocalBlockRetriever(
      String execId,
      String appId,
      String[] blockIds,
      RemoteBlockHandler blockHandler) {
    this.blockIds = blockIds;
    this.appId = appId;
    this.blockHandler = blockHandler;
    this.execId = execId;
    if (blockIds.length == 0) {
      throw new IllegalArgumentException("Zero-sized blockIds array");
    }
    this.message = createFetchShuffleBlocksMsg(appId, execId, blockIds);
  }


  /**
   * Analyze the pass in blockIds and create FetchShuffleBlocks message.
   * The blockIds has been sorted by mapId and reduceId. It's produced in
   * org.apache.spark.MapOutputTracker.convertMapStatuses.
   */
  private FetchShuffleBlocks createFetchShuffleBlocksMsg(
      String appId, String execId, String[] blockIds) {
    String[] firstBlock = splitBlockId(blockIds[0]);
    int shuffleId = Integer.parseInt(firstBlock[1]);
    boolean batchFetchEnabled = firstBlock.length == 5;

    HashMap<Long, ArrayList<Integer>> mapIdToReduceIds = new HashMap<>();
    for (String blockId : blockIds) {
      String[] blockIdParts = splitBlockId(blockId);
      if (Integer.parseInt(blockIdParts[1]) != shuffleId) {
        throw new IllegalArgumentException("Expected shuffleId=" + shuffleId +
          ", got:" + blockId);
      }
      long mapId = Long.parseLong(blockIdParts[2]);
      if (!mapIdToReduceIds.containsKey(mapId)) {
        mapIdToReduceIds.put(mapId, new ArrayList<>());
      }
      mapIdToReduceIds.get(mapId).add(Integer.parseInt(blockIdParts[3]));
      if (batchFetchEnabled) {
        // When we read continuous shuffle blocks in batch, we will reuse reduceIds in
        // FetchShuffleBlocks to store the start and end reduce id for range
        // [startReduceId, endReduceId).
        assert(blockIdParts.length == 5);
        mapIdToReduceIds.get(mapId).add(Integer.parseInt(blockIdParts[4]));
      }
    }
    long[] mapIds = Longs.toArray(mapIdToReduceIds.keySet());
    int[][] reduceIdArr = new int[mapIds.length][];
    for (int i = 0; i < mapIds.length; i++) {
      reduceIdArr[i] = Ints.toArray(mapIdToReduceIds.get(mapIds[i]));
    }
    return new FetchShuffleBlocks(
      appId, execId, shuffleId, mapIds, reduceIdArr, batchFetchEnabled);
  }

  /** Split the shuffleBlockId and return shuffleId, mapId and reduceIds. */
  private String[] splitBlockId(String blockId) {
    String[] blockIdParts = blockId.split("_");
    // For batch block id, the format contains shuffleId, mapId, begin reduceId, end reduceId.
    // For single block id, the format contains shuffleId, mapId, educeId.
    if (blockIdParts.length < 4 || blockIdParts.length > 5 || !blockIdParts[0].equals("shuffle")) {
      throw new IllegalArgumentException(
        "Unexpected shuffle block id format: " + blockId);
    }
    return blockIdParts;
  }


  /**
   * Begins the local retrieving process, calling the listener with every block retrieved.
   */
  public void start() {
    logger.trace("starting block retrieval.");
    blockHandler.retrieveLocalBLocksForUpcomingTasks(message);

  }

}
