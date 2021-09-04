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

import com.codahale.metrics.*;
import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.StreamCallbackWithID;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.shuffle.RemoteShuffleBlockResolver.AppExecId;
import org.apache.spark.network.shuffle.protocol.*;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;


/**
 * RPC Handler for a server which can serve both RDD blocks and shuffle blocks from outside
 * of an Executor process.
 *
 * Handles registering executors and opening shuffle or disk persisted RDD blocks from them.
 * Blocks are registered with the "one-for-one" strategy, meaning each Transport-layer Chunk
 * is equivalent to one block.
 */
public class RemoteBlockHandler extends RpcHandler {
  private static final Logger logger = LoggerFactory.getLogger(RemoteBlockHandler.class);

  @VisibleForTesting
  final RemoteShuffleBlockResolver blockManager;
  private final OneForOneStreamManager streamManager;
  private final ShuffleMetrics metrics;
  private final MergedShuffleFileManager mergeManager;
  public static String shuffleServiceAvailability;

  public RemoteBlockHandler(TransportConf conf, File registeredExecutorFile, boolean lookAheadCachingEnabled, int lookAheadCachingSize)
    throws IOException {
    this(new OneForOneStreamManager(),
      new RemoteShuffleBlockResolver(conf, registeredExecutorFile, lookAheadCachingEnabled, lookAheadCachingSize),
      new NoOpMergedShuffleFileManager());
  }

  public RemoteBlockHandler(
      TransportConf conf,
      File registeredExecutorFile,
      MergedShuffleFileManager mergeManager) throws IOException {
    this(new OneForOneStreamManager(),
      new RemoteShuffleBlockResolver(conf, registeredExecutorFile), mergeManager);
  }

  @VisibleForTesting
  public RemoteShuffleBlockResolver getBlockResolver() {
    return blockManager;
  }

  /** Enables mocking out the StreamManager and BlockManager. */
  @VisibleForTesting
  public RemoteBlockHandler(
      OneForOneStreamManager streamManager,
      RemoteShuffleBlockResolver blockManager) {
    this(streamManager, blockManager, new NoOpMergedShuffleFileManager());
  }

  /** Enables mocking out the StreamManager, BlockManager, and MergeManager. */
  @VisibleForTesting
  public RemoteBlockHandler(
      OneForOneStreamManager streamManager,
      RemoteShuffleBlockResolver blockManager,
      MergedShuffleFileManager mergeManager) {
    this.metrics = new ShuffleMetrics();
    this.streamManager = streamManager;
    this.blockManager = blockManager;
    this.mergeManager = mergeManager;
  }

  public RemoteBlockHandler(
          OneForOneStreamManager streamManager,
          RemoteShuffleBlockResolver blockManager,
          NoOpMergedShuffleFileManager mergeManager) {
    this.metrics = new ShuffleMetrics();
    this.streamManager = streamManager;
    this.blockManager = blockManager;
    this.mergeManager = mergeManager;
  }

  @Override
  public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
    BlockTransferMessage msgObj = BlockTransferMessage.Decoder.fromByteBuffer(message);
    handleMessage(msgObj, client, callback);
  }

  @Override
  public StreamCallbackWithID receiveStream(
      TransportClient client,
      ByteBuffer messageHeader,
      RpcResponseCallback callback) {
    BlockTransferMessage msgObj = BlockTransferMessage.Decoder.fromByteBuffer(messageHeader);
    //logger.trace("msgObj: "+msgObj);
    //if (msgObj instanceof PushBlockStream) {
    //  PushBlockStream message = (PushBlockStream) msgObj;
    if (msgObj instanceof UploadBlockStream) {
      UploadBlockStream message = (UploadBlockStream) msgObj;
      //checkAuth(client, message.appId);
      try {
        return mergeManager.receiveBlockDataAsStream(message, blockManager);
      } catch (Exception e) {
        e.printStackTrace();
        throw new UnsupportedOperationException("Cannot handle shuffle block merge");
      }
    } else {
      throw new UnsupportedOperationException("Unexpected message with #receiveStream: " + msgObj);
    }
  }

  protected void handleMessage(
      BlockTransferMessage msgObj,
      TransportClient client,
      RpcResponseCallback callback) {
    if (msgObj instanceof FetchShuffleBlocks || msgObj instanceof OpenBlocks) {
      final Timer.Context responseDelayContext = metrics.openBlockRequestLatencyMillis.time();
      try {
        int numBlockIds;
        long streamId;
        if (msgObj instanceof FetchShuffleBlocks) {
          FetchShuffleBlocks msg = (FetchShuffleBlocks) msgObj;
          checkAuth(client, msg.appId);
          numBlockIds = 0;
          if (msg.batchFetchEnabled) {
            numBlockIds = msg.mapIds.length;
          } else {
            for (int[] ids: msg.reduceIds) {
              numBlockIds += ids.length;
            }
          }
          streamId = streamManager.registerStream(client.getClientId(),
            new ShuffleManagedBufferIterator(msg), client.getChannel());
        } else {
          // For the compatibility with the old version, still keep the support for OpenBlocks.
          OpenBlocks msg = (OpenBlocks) msgObj;
          numBlockIds = msg.blockIds.length;
          checkAuth(client, msg.appId);
          streamId = streamManager.registerStream(client.getClientId(),
            new ManagedBufferIterator(msg), client.getChannel());
        }
        if (logger.isTraceEnabled()) {
          logger.trace(
            "Registered streamId {} with {} buffers for client {} from host {}",
            streamId,
            numBlockIds,
            client.getClientId(),
            getRemoteAddress(client.getChannel()));
        }
        callback.onSuccess(new StreamHandle(streamId, numBlockIds).toByteBuffer());
      } finally {
        responseDelayContext.stop();
      }

    } else if (msgObj instanceof RegisterExecutor) {
      final Timer.Context responseDelayContext =
        metrics.registerExecutorRequestLatencyMillis.time();
      try {
        RegisterExecutor msg = (RegisterExecutor) msgObj;
        checkAuth(client, msg.appId);
        blockManager.registerExecutor(msg.appId, msg.execId, msg.executorInfo);
        mergeManager.registerExecutor(msg.appId, msg.executorInfo.localDirs);
        callback.onSuccess(ByteBuffer.wrap(new byte[0]));
      } finally {
        responseDelayContext.stop();
      }

    } else if (msgObj instanceof RemoveBlocks) {
      RemoveBlocks msg = (RemoveBlocks) msgObj;
      checkAuth(client, msg.appId);
      int numRemovedBlocks = blockManager.removeBlocks(msg.appId, msg.execId, msg.blockIds);
      callback.onSuccess(new BlocksRemoved(numRemovedBlocks).toByteBuffer());

    } else if (msgObj instanceof GetLocalDirsForExecutors) {
      GetLocalDirsForExecutors msg = (GetLocalDirsForExecutors) msgObj;
      checkAuth(client, msg.appId);
      Map<String, String[]> localDirs = blockManager.getLocalDirs(msg.appId, msg.execIds);
      callback.onSuccess(new LocalDirsForExecutors(localDirs).toByteBuffer());

    } else if (msgObj instanceof FinalizeShuffleMerge) {
      final Timer.Context responseDelayContext =
          metrics.finalizeShuffleMergeLatencyMillis.time();
      FinalizeShuffleMerge msg = (FinalizeShuffleMerge) msgObj;
      try {
        checkAuth(client, msg.appId);
        MergeStatuses statuses = mergeManager.finalizeShuffleMerge(msg);
        callback.onSuccess(statuses.toByteBuffer());
      } catch(IOException e) {
        throw new RuntimeException(String.format("Error while finalizing shuffle merge "
          + "for application %s shuffle %d", msg.appId, msg.shuffleId), e);
      } finally {
        responseDelayContext.stop();
      }
    } else if (msgObj instanceof UploadBlock) {
      UploadBlock msg = (UploadBlock) msgObj;
      checkAuth(client, msg.appId);
      logger.trace("msg blockData: "+ Arrays.toString(msg.blockData));
      logger.trace("msg appId: "+msg.appId);
      logger.trace("msg Meta: "+ Arrays.toString(msg.metadata));
      String meta = JavaUtils.bytesToString(ByteBuffer.wrap(msg.metadata));
      logger.trace("Meta: {}", meta);
      logger.trace("msg blockId: "+msg.blockId);
      callback.onSuccess(ByteBuffer.wrap(new byte[0]));
  } else if (msgObj instanceof DeregisterExecutor) {
      try {
        DeregisterExecutor msg = (DeregisterExecutor) msgObj;
        checkAuth(client, msg.appId);
        blockManager.deregisterExecutor(msg.appId, msg.execId);
        callback.onSuccess(ByteBuffer.wrap(new byte[0]));
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      throw new UnsupportedOperationException("Unexpected message: " + msgObj);
    }
  }

  @Override
  public void exceptionCaught(Throwable cause, TransportClient client) {
    metrics.caughtExceptions.inc();
  }

  public MetricSet getAllMetrics() {
    return metrics;
  }

  @Override
  public StreamManager getStreamManager() {
    return streamManager;
  }

  /**
   * Removes an application (once it has been terminated), and optionally will clean up any
   * local directories associated with the executors of that application in a separate thread.
   */
  public void applicationRemoved(String appId, boolean cleanupLocalDirs) {
    blockManager.applicationRemoved(appId, cleanupLocalDirs);
  }

  /**
   * Clean up any non-shuffle files in any local directories associated with an finished executor.
   */
  public void executorRemoved(String executorId, String appId) {
    blockManager.executorRemoved(executorId, appId);
  }

  /**
   * Register an (application, executor) with the given shuffle info.
   *
   * The "re-" is meant to highlight the intended use of this method -- when this service is
   * restarted, this is used to restore the state of executors from before the restart.  Normal
   * registration will happen via a message handled in receive()
   *
   * @param appExecId
   * @param executorInfo
   */
  public void reregisterExecutor(AppExecId appExecId, ExecutorShuffleInfo executorInfo) {
    blockManager.registerExecutor(appExecId.appId, appExecId.execId, executorInfo);
  }

  public void close() {
    blockManager.close();
  }

  private void checkAuth(TransportClient client, String appId) {
    if (client.getClientId() != null && !client.getClientId().equals(appId)) {
      throw new SecurityException(String.format(
        "Client for %s not authorized for application %s.", client.getClientId(), appId));
    }
  }

  /**
   * A simple class to wrap all shuffle service wrapper metrics
   */
  @VisibleForTesting
  public class ShuffleMetrics implements MetricSet {
    private final Map<String, Metric> allMetrics;
    // Time latency for open block request in ms
    private final Timer openBlockRequestLatencyMillis = new Timer();
    // Time latency for executor registration latency in ms
    private final Timer registerExecutorRequestLatencyMillis = new Timer();
    // Time latency for processing finalize shuffle merge request latency in ms
    private final Timer finalizeShuffleMergeLatencyMillis = new Timer();
    // Block transfer rate in byte per second
    private final Meter blockTransferRateBytes = new Meter();
    // Number of active connections to the shuffle service
    private Counter activeConnections = new Counter();
    // Number of exceptions caught in connections to the shuffle service
    private Counter caughtExceptions = new Counter();

    public ShuffleMetrics() {
      allMetrics = new HashMap<>();
      allMetrics.put("openBlockRequestLatencyMillis", openBlockRequestLatencyMillis);
      allMetrics.put("registerExecutorRequestLatencyMillis", registerExecutorRequestLatencyMillis);
      allMetrics.put("finalizeShuffleMergeLatencyMillis", finalizeShuffleMergeLatencyMillis);
      allMetrics.put("blockTransferRateBytes", blockTransferRateBytes);
      allMetrics.put("registeredExecutorsSize",
                     (Gauge<Integer>) () -> blockManager.getRegisteredExecutorsSize());
      allMetrics.put("numActiveConnections", activeConnections);
      allMetrics.put("numCaughtExceptions", caughtExceptions);
    }

    @Override
    public Map<String, Metric> getMetrics() {
      return allMetrics;
    }
  }

  private class ManagedBufferIterator implements Iterator<ManagedBuffer> {

    private int index = 0;
    private final Function<Integer, ManagedBuffer> blockDataForIndexFn;
    private final int size;

    ManagedBufferIterator(OpenBlocks msg) {
      String appId = msg.appId;
      String execId = msg.execId;
      String[] blockIds = msg.blockIds;
      String[] blockId0Parts = blockIds[0].split("_");
      if (blockId0Parts.length == 4 && blockId0Parts[0].equals("shuffle")) {
        final int shuffleId = Integer.parseInt(blockId0Parts[1]);
        final int[] mapIdAndReduceIds = shuffleMapIdAndReduceIds(blockIds, shuffleId);
        size = mapIdAndReduceIds.length;
        blockDataForIndexFn = index -> blockManager.getBlockData(appId, execId, shuffleId,
          mapIdAndReduceIds[index], mapIdAndReduceIds[index + 1]);
      } else if (blockId0Parts.length == 3 && blockId0Parts[0].equals("rdd")) {
        final int[] rddAndSplitIds = rddAndSplitIds(blockIds);
        size = rddAndSplitIds.length;
        blockDataForIndexFn = index -> blockManager.getRddBlockData(appId, execId,
          rddAndSplitIds[index], rddAndSplitIds[index + 1]);
      } else {
        throw new IllegalArgumentException("Unexpected block id format: " + blockIds[0]);
      }
    }

    private int[] rddAndSplitIds(String[] blockIds) {
      final int[] rddAndSplitIds = new int[2 * blockIds.length];
      for (int i = 0; i < blockIds.length; i++) {
        String[] blockIdParts = blockIds[i].split("_");
        if (blockIdParts.length != 3 || !blockIdParts[0].equals("rdd")) {
          throw new IllegalArgumentException("Unexpected RDD block id format: " + blockIds[i]);
        }
        rddAndSplitIds[2 * i] = Integer.parseInt(blockIdParts[1]);
        rddAndSplitIds[2 * i + 1] = Integer.parseInt(blockIdParts[2]);
      }
      return rddAndSplitIds;
    }

    private int[] shuffleMapIdAndReduceIds(String[] blockIds, int shuffleId) {
      final int[] mapIdAndReduceIds = new int[2 * blockIds.length];
      for (int i = 0; i < blockIds.length; i++) {
        String[] blockIdParts = blockIds[i].split("_");
        if (blockIdParts.length != 4 || !blockIdParts[0].equals("shuffle")) {
          throw new IllegalArgumentException("Unexpected shuffle block id format: " + blockIds[i]);
        }
        if (Integer.parseInt(blockIdParts[1]) != shuffleId) {
          throw new IllegalArgumentException("Expected shuffleId=" + shuffleId +
            ", got:" + blockIds[i]);
        }
        mapIdAndReduceIds[2 * i] = Integer.parseInt(blockIdParts[2]);
        mapIdAndReduceIds[2 * i + 1] = Integer.parseInt(blockIdParts[3]);
      }
      return mapIdAndReduceIds;
    }

    @Override
    public boolean hasNext() {
      return index < size;
    }

    @Override
    public ManagedBuffer next() {
      final ManagedBuffer block = blockDataForIndexFn.apply(index);
      index += 2;
      metrics.blockTransferRateBytes.mark(block != null ? block.size() : 0);
      return block;
    }
  }

  private class ShuffleManagedBufferIterator implements Iterator<ManagedBuffer> {

    private int mapIdx = 0;
    private int reduceIdx = 0;

    private final String appId;
    private final String execId;
    private final int shuffleId;
    private final long[] mapIds;
    private final int[][] reduceIds;
    private final boolean batchFetchEnabled;

    ShuffleManagedBufferIterator(FetchShuffleBlocks msg) {
      appId = msg.appId;
      execId = msg.execId;
      shuffleId = msg.shuffleId;
      mapIds = msg.mapIds;
      reduceIds = msg.reduceIds;
      batchFetchEnabled = msg.batchFetchEnabled;
    }

    @Override
    public boolean hasNext() {
      // mapIds.length must equal to reduceIds.length, and the passed in FetchShuffleBlocks
      // must have non-empty mapIds and reduceIds, see the checking logic in
      // OneForOneBlockFetcher.
      assert(mapIds.length != 0 && mapIds.length == reduceIds.length);
      return mapIdx < mapIds.length && reduceIdx < reduceIds[mapIdx].length;
    }

    @Override
    public ManagedBuffer next() {
      ManagedBuffer block;
      long start = System.currentTimeMillis();
      if (!batchFetchEnabled) {
        block = blockManager.getBlockData(
          appId, execId, shuffleId, mapIds[mapIdx], reduceIds[mapIdx][reduceIdx]);
        if (reduceIdx < reduceIds[mapIdx].length - 1) {
          reduceIdx += 1;
        } else {
          reduceIdx = 0;
          mapIdx += 1;
        }
      } else {
        assert(reduceIds[mapIdx].length == 2);
        block = blockManager.getContinuousBlocksData(appId, execId, shuffleId, mapIds[mapIdx],
          reduceIds[mapIdx][0], reduceIds[mapIdx][1]);
        mapIdx += 1;
      }
      long elapsedTime = System.currentTimeMillis()-start;
      logger.trace("TIME TO GET BLOCK(ms): "+elapsedTime);
      metrics.blockTransferRateBytes.mark(block != null ? block.size() : 0);
      return block;
    }
  }


  private class ShuffleManagedRetrieveBlockBufferIterator implements Iterator<ManagedBuffer> {

    private int mapIdx = 0;
    private int reduceIdx = 0;

    private final String appId;
    private final String execId;
    private final int shuffleId;
    private final long[] mapIds;
    private final int[][] reduceIds;
    private final boolean batchFetchEnabled;

    ShuffleManagedRetrieveBlockBufferIterator(FetchShuffleBlocks msg) {
      appId = msg.appId;
      execId = msg.execId;
      shuffleId = msg.shuffleId;
      mapIds = msg.mapIds;
      reduceIds = msg.reduceIds;
      batchFetchEnabled = msg.batchFetchEnabled;
    }

    @Override
    public boolean hasNext() {
      // mapIds.length must equal to reduceIds.length, and the passed in FetchShuffleBlocks
      // must have non-empty mapIds and reduceIds, see the checking logic in
      // OneForOneBlockFetcher.
      assert(mapIds.length != 0 && mapIds.length == reduceIds.length);
      return mapIdx < mapIds.length && reduceIdx < reduceIds[mapIdx].length;
    }

    @Override
    public ManagedBuffer next() {
      ManagedBuffer block;
      block = blockManager.retrieveBlockDataAndStoreInCache(
              appId, execId, shuffleId, mapIds[mapIdx], reduceIds[mapIdx][reduceIdx], reduceIds[mapIdx][reduceIdx]+1);
      if (reduceIdx < reduceIds[mapIdx].length - 1) {
        reduceIdx += 1;
      } else {
        reduceIdx = 0;
        mapIdx += 1;
      }
      return block;
    }
  }


  public void retrieveLocalBLocksForUpcomingTasks(FetchShuffleBlocks msg){
    logger.trace("REMOTEBLOCKHANDLER " + msg);
    Iterator<ManagedBuffer> buffers = new ShuffleManagedRetrieveBlockBufferIterator(msg);

    while(buffers.hasNext()){
     ManagedBuffer nextChunk = buffers.next();
      nextChunk.release();
    }
    //rpcResponseCallback.onSuccess(ByteBuffer.allocate(0));
  }

  /**
   * Dummy implementation of merged shuffle file manager. Suitable for when push-based shuffle
   * is not enabled.
   */
  private static class NoOpMergedShuffleFileManager implements MergedShuffleFileManager {

    @Override
    public StreamCallbackWithID receiveBlockDataAsStream(UploadBlockStream message){
      throw new UnsupportedOperationException("Cannot handle shuffle block merge");
    }

    @Override
    public StreamCallbackWithID receiveBlockDataAsStream(UploadBlockStream msg, ExternalShuffleBlockResolver blockManager) {
      return null;
    }

    @Override
    public StreamCallbackWithID receiveBlockDataAsStream(UploadBlockStream message, RemoteShuffleBlockResolver blockManager){
      logger.trace("Message arrived: "+message);
      //20/11/29 15:49:30 TRACE ExternalBlockHandler: Message arrived: PushBlockStream{appId=app-20201129154921-0003,
      // blockId=/tmp/spark-7c7f98b9-8fb1-41fb-acfd-999f2e0a0f95/executor-42225f0d-059c-42b3-97b0-4edac95b0afe/blockmgr-7eca8a4b-d0cd-4adc-bb19-1cbb9d9a00a9/0c/shuffle_0_0_0.data,
      // index=0}
      //throw new UnsupportedOperationException("Cannot handle shuffle block merge");
      //return null;

      //
      try{
        String meta = JavaUtils.bytesToString(ByteBuffer.wrap(message.metadata));
        //logger.trace("Meta: {}", meta);
      }
      catch (Exception e){
        logger.trace("Exception: ", e);
      }
      /*
      val diskBlockManager = { new DiskBlockManager(sparkConf, true) }
      Serializer serializerManager = SparkEnv.get.serializerManager//new SerializerManager()

      val (level, classTag) = deserializeMetadata(message.`metadata, sparkConf)
      logTrace(s"level: $level, classTag: $classTag")*/
      String blockDataFile = message.blockId;
      String[] blockIdPath = blockDataFile.split("/");
      String blockIdrem = blockIdPath[blockIdPath.length-1];
      System.out.println(blockIdrem);

      String[] rem = null;
      if(blockIdrem.contains("data")){
        rem = blockIdrem.split(".data");
      }
      else if(blockIdrem.contains("index")){
        rem = blockIdrem.split(".index");
      }
      System.out.println(Arrays.toString(rem));
      assert rem != null;
      final String blockId = rem[0];
      //logger.trace("Getting Block with id: {}",blockId);

      //val (_, tmpFile) = diskBlockManager.createTempLocalBlock()
      //String tmpFile = new File(blockManager.executors.get('f'));
      // to get first executor info for testing
      Map.Entry<AppExecId, ExecutorShuffleInfo> entry = blockManager.executors.entrySet().iterator().next();
      AppExecId id = entry.getKey();
      ExecutorShuffleInfo info = entry.getValue();

      // na check to IOUtil.writeFile(String content, File file)
      OutputStream outputStream = null;
      String tmpFile = null;
      CountingWritableChannel channel = null;

      //-Path currentTmpPath = null;

      try{
        //outputStream = Files.newOutputStream(Paths.get(info.localDirs[0])); //(Paths.get(info.localDirs[0]));
        //tmpFile = info.localDirs[0]+blockId+".data";
        int sepPos = blockDataFile.lastIndexOf("/");
        String tmpPath = blockDataFile.substring(0,sepPos);
        //JavaUtils.createDirectory(new File(tmpPath));

        JavaUtils.createNIODirectory(Paths.get(tmpPath));

        tmpFile = blockIdrem;
        channel = new CountingWritableChannel(
                Channels.newChannel(new FileOutputStream(tmpPath + "/" + tmpFile)));

        //Channels.newChannel(new FileOutputStream(outputStream)));
      }
      catch (Exception e){
        e.printStackTrace();
      }

      logger.trace("Writing block {} to tmp file {}", blockId, tmpFile);
      //

      RemoteBlockHandler.CountingWritableChannel finalChannel = channel;

      String finalTmpFile = tmpFile;
      //-Path finalCurrentTmpPath = currentTmpPath;
      //-String finalTmpFile1 = tmpFile;
      return new StreamCallbackWithID() {
        @Override
        public void onData(String streamId, ByteBuffer buf) throws IOException {
          //logger.trace("onData");
          while (buf.hasRemaining()) {
            //logger.trace("Buf data arrived: {}",buf.toString());
            finalChannel.write(buf);
          }
        }
        @Override
        public void onComplete(String streamId) throws IOException {
          //logger.trace("onComplete");
          finalChannel.close();
          Long blockSize = finalChannel.getCount();
          logger.trace("Block size of received {} file (bytes): {}", finalTmpFile, blockSize);

          //
        /*  if (finalTmpFile.contains(".data")){
            Runnable runnable =
                    () -> {
                      String dataFile = blockDataFile.substring(0,blockDataFile.lastIndexOf("/")) + "/" + finalTmpFile;
                      logger.info("Adding block data in LoadingCache... for {}", dataFile);
                      try {

                        blockManager.dataCache.put(new File(dataFile), new ShuffleDataInformation(new File(dataFile)));*/
                        /*
                        int size = (int)new File(dataFile).length();
                        ByteBuffer buffer = ByteBuffer.allocate(size);//allocateDirect(size); // // = buffer.asLongBuffer();
                        DataInputStream dis = new DataInputStream(Files.newInputStream(new File(dataFile).toPath()));
                        dis.readFully(buffer.array()); //readFully(buffer.array(), 0, size); //
                        dis.close();
                        blockManager.dataCache.put(dataFile, buffer.array());
                        */
                        //blockManager.shuffleDataCache.put( new File(dataFile), new ShuffleDataInformation(new File(dataFile)));
            /*          } catch (IOException e) {
                        e.printStackTrace();
                      }
                    };
            Thread thread = new Thread(runnable);
            thread.start();
          }*/

          //val blockStored = TempFileBasedBlockStoreUpdater(message.blockId, level, classTag, tmpFile, blockSize).save
          //if (!blockStored) {
          //  throw new Exception(s"Failure while trying to store block ${message.blockId} on $blockManagerId.")
          //}
        }
        @Override
        public void onFailure(String streamId, Throwable cause) throws IOException {
          logger.trace("Failure");
          finalChannel.close();
          //tmpFile.delete();
        }
        @Override
        public String getID() {
          //return message.blockId;
          return blockId;
        }
      };
    }

    @Override
    public MergeStatuses finalizeShuffleMerge(FinalizeShuffleMerge msg) throws IOException {
      throw new UnsupportedOperationException("Cannot handle shuffle block merge");
    }

    @Override
    public void registerApplication(String appId, String user) {
      // No-op. Do nothing.
    }

    @Override
    public void registerExecutor(String appId, String[] localDirs) {
      // No-Op. Do nothing.
    }

    @Override
    public void applicationRemoved(String appId, boolean cleanupLocalDirs) {
      throw new UnsupportedOperationException("Cannot handle shuffle block merge");
    }

    @Override
    public ManagedBuffer getMergedBlockData(
        String appId, int shuffleId, int reduceId, int chunkId) {
      throw new UnsupportedOperationException("Cannot handle shuffle block merge");
    }

    @Override
    public MergedBlockMeta getMergedBlockMeta(String appId, int shuffleId, int reduceId) {
      throw new UnsupportedOperationException("Cannot handle shuffle block merge");
    }

    @Override
    public String[] getMergedBlockDirs(String appId) {
      throw new UnsupportedOperationException("Cannot handle shuffle block merge");
    }
  }

  @Override
  public void channelActive(TransportClient client) {
    metrics.activeConnections.inc();
    super.channelActive(client);
  }

  @Override
  public void channelInactive(TransportClient client) {
    metrics.activeConnections.dec();
    super.channelInactive(client);
  }


  //
  /*private def deserializeMetadata[T](metadata: Array[Byte], sparkConf: SparkConf): (StorageLevel, ClassTag[T]) = {
    val serializer = new JavaSerializer(sparkConf)
    serializer
            .newInstance()
            .deserialize(ByteBuffer.wrap(metadata))
            .asInstanceOf[(StorageLevel, ClassTag[T])]
  }*/

  private static class CountingWritableChannel implements WritableByteChannel {
    final private WritableByteChannel sink;
    public CountingWritableChannel(
            WritableByteChannel sink) {
      this.sink = sink;
    }

    private Long count = 0L;

    public Long getCount(){
      return count;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
      int written =sink.write(src);
      if (written > 0) {
        count += written;
      }
      return written;
    }

    @Override
    public boolean isOpen(){
      return sink.isOpen();
    }

    @Override
    public void close() throws IOException {
      sink.close();
    }
//

  }



}
