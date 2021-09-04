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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NettyManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.LevelDBProvider;
import org.apache.spark.network.util.LevelDBProvider.StoreVersion;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Manages converting shuffle BlockIds into physical segments of local files, from a process outside
 * of Executors. Each Executor must register its own configuration about where it stores its files
 * (local dirs) and how (shuffle manager). The logic for retrieval of individual files is replicated
 * from Spark's IndexShuffleBlockResolver.
 */
public class RemoteShuffleBlockResolver{
  private static final Logger logger = LoggerFactory.getLogger(RemoteShuffleBlockResolver.class);

  private static final ObjectMapper mapper = new ObjectMapper();

  /**
   * This a common prefix to the key for each app registration we stick in leveldb, so they
   * are easy to find, since leveldb lets you search based on prefix.
   */
  private static final String APP_KEY_PREFIX = "AppExecShuffleInfo";
  private static final StoreVersion CURRENT_VERSION = new StoreVersion(1, 0);

  // Map containing all registered executors' metadata.
  @VisibleForTesting
  final ConcurrentMap<AppExecId, ExecutorShuffleInfo> executors;

  /**
   *  Caches index file information so that we can avoid open/close the index files
   *  for each block fetch.
   */
  private final LoadingCache<File, ShuffleIndexInformation> shuffleIndexCache;

  //---------
  /**
   *  Caches data file information so that we can avoid open/close the data files
   *  for each block fetch.
   */
  //final LoadingCache<String, ByteBuffer> shuffleDataCache; //private
  private final LoadingCache<List<Object>, ShufflePartitionInformation> shuffleDataCache;
  final LinkedHashMap<String, ManagedBuffer > dataCache; //File , byte[] ShuffleDataInformation
  //final ConcurrentHashMap<String, ManagedBuffer > dataCache; //File , byte[] ShuffleDataInformation
  //--------

  // Single-threaded Java executor used to perform expensive recursive directory deletion.
  private final Executor directoryCleaner;

  final TransportConf conf;

  private final boolean rddFetchEnabled;

  @VisibleForTesting
  final File registeredExecutorFile;
  @VisibleForTesting
  final DB db;


  private String dataBlockCacheKeyToPut;
  private String dataBlockCacheKeyToGet;
  private final Object lock = new Object();
  //-private final ArrayList<Integer> test = new ArrayList<Integer>();

  final boolean lookAheadCachingEnabled;
  final int lookAheadCachingSize;

  public RemoteShuffleBlockResolver(TransportConf conf, File registeredExecutorFile)
      throws IOException {
    this(conf, registeredExecutorFile, Executors.newSingleThreadExecutor(
        // Add `spark` prefix because it will run in NM in Yarn mode.
        NettyUtils.createThreadFactory("spark-shuffle-directory-cleaner")));
  }

  public RemoteShuffleBlockResolver(TransportConf conf, File registeredExecutorFile, boolean lookAheadCachingEnabled, int lookAheadCachingSize)
          throws IOException {
    this(conf, registeredExecutorFile, Executors.newSingleThreadExecutor(
            // Add `spark` prefix because it will run in NM in Yarn mode.
            NettyUtils.createThreadFactory("spark-shuffle-directory-cleaner")), lookAheadCachingEnabled, lookAheadCachingSize);
  }
  // Allows tests to have more control over when directories are cleaned up.
  @VisibleForTesting
  RemoteShuffleBlockResolver(
      TransportConf conf,
      File registeredExecutorFile,
      Executor directoryCleaner) throws IOException {
    this.conf = conf;
    this.rddFetchEnabled =
      Boolean.valueOf(conf.get(Constants.SHUFFLE_SERVICE_FETCH_RDD_ENABLED, "false"));
    this.registeredExecutorFile = registeredExecutorFile;
    String indexCacheSize = conf.get("spark.shuffle.service.index.cache.size", "100m");
    CacheLoader<File, ShuffleIndexInformation> indexCacheLoader =
        new CacheLoader<File, ShuffleIndexInformation>() {
          public ShuffleIndexInformation load(File file) throws IOException {
            return new ShuffleIndexInformation(file);
          }
        };
    shuffleIndexCache = CacheBuilder.newBuilder()
      .maximumWeight(JavaUtils.byteStringAsBytes(indexCacheSize))
      .weigher(new Weigher<File, ShuffleIndexInformation>() {
        public int weigh(File file, ShuffleIndexInformation indexInfo) {
          return indexInfo.getSize();
        }
      })
      .build(indexCacheLoader);

    //
    dataCache = null;
    lookAheadCachingSize = 0;
    lookAheadCachingEnabled = false;
    shuffleDataCache = null;
    //

    db = LevelDBProvider.initLevelDB(this.registeredExecutorFile, CURRENT_VERSION, mapper);
    if (db != null) {
      executors = reloadRegisteredExecutors(db);
    } else {
      executors = Maps.newConcurrentMap();
    }
    this.directoryCleaner = directoryCleaner;
  }

  RemoteShuffleBlockResolver(
          TransportConf conf,
          File registeredExecutorFile,
          Executor directoryCleaner,
          boolean lookAheadCachingEnabled,
          int lookAheadCachingSize) throws IOException {
    this.conf = conf;
    this.lookAheadCachingEnabled = lookAheadCachingEnabled;
    this.lookAheadCachingSize = lookAheadCachingSize;
    this.rddFetchEnabled =
            Boolean.valueOf(conf.get(Constants.SHUFFLE_SERVICE_FETCH_RDD_ENABLED, "false"));
    this.registeredExecutorFile = registeredExecutorFile;
    String indexCacheSize = conf.get("spark.shuffle.service.index.cache.size", "100m");
    CacheLoader<File, ShuffleIndexInformation> indexCacheLoader =
            new CacheLoader<File, ShuffleIndexInformation>() {
              public ShuffleIndexInformation load(File file) throws IOException {
                return new ShuffleIndexInformation(file);
              }
            };
    shuffleIndexCache = CacheBuilder.newBuilder()
            .maximumWeight(JavaUtils.byteStringAsBytes(indexCacheSize))
            .weigher(new Weigher<File, ShuffleIndexInformation>() {
              public int weigh(File file, ShuffleIndexInformation indexInfo) {
                return indexInfo.getSize();
              }
            })
            .build(indexCacheLoader);


    //------
    /*String dataCacheSize = String.valueOf(lookAheadCachingSize)+"m";
    CacheLoader<List<Object>, ShufflePartitionInformation> dataCacheLoader =
            new CacheLoader<List<Object>, ShufflePartitionInformation>() {
              public ShufflePartitionInformation load(List list) throws IOException {
                return new ShufflePartitionInformation(list);
              }
            };
    shuffleDataCache = CacheBuilder.newBuilder()
            .maximumWeight(JavaUtils.byteStringAsBytes(dataCacheSize)) //.maximumSize(300)
            .weigher(new Weigher<List<Object>, ShufflePartitionInformation>() {
              public int weigh(List<Object> list, ShufflePartitionInformation dataInfo) {
                return dataInfo.getSize();
              }
            })
            .build(dataCacheLoader);
    logger.info("Size of cache available: "+dataCacheSize);*/
    //------

    if (lookAheadCachingEnabled){
      int maxEntries = this.lookAheadCachingSize; //15000; //300;
      dataCache = new // File, byte[] ShuffleDataInformation
              LinkedHashMap<String, ManagedBuffer>(maxEntries*10/7, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, ManagedBuffer> eldest) {
                  return size() > maxEntries;
                }
              };
      logger.info("Size of cache available: "+lookAheadCachingSize);

      /*dataCache = new // File, byte[] ShuffleDataInformation
              ConcurrentHashMap<String, ManagedBuffer>(maxEntries*10/7, 0.75f, 8) {};
      logger.info("Size of cache available: "+lookAheadCachingSize);*/

    }
    else{
      dataCache = null;
    }
    shuffleDataCache = null;
    //dataCache = null;
    //------

    dataBlockCacheKeyToPut = null;
    dataBlockCacheKeyToGet = null;
    //bufToStoreInCache = null;


    db = LevelDBProvider.initLevelDB(this.registeredExecutorFile, CURRENT_VERSION, mapper);
    if (db != null) {
      executors = reloadRegisteredExecutors(db);
    } else {
      executors = Maps.newConcurrentMap();
    }
    this.directoryCleaner = directoryCleaner;
  }



  public int getRegisteredExecutorsSize() {
    return executors.size();
  }

  /** Registers a new Executor with all the configuration we need to find its shuffle files. */
  public void registerExecutor(
      String appId,
      String execId,
      ExecutorShuffleInfo executorInfo) {
    AppExecId fullId = new AppExecId(appId, execId);
    logger.info("Registered executor {} with {}", fullId, executorInfo);
    try {
      if (db != null) {
        byte[] key = dbAppExecKey(fullId);
        byte[] value = mapper.writeValueAsString(executorInfo).getBytes(StandardCharsets.UTF_8);
        db.put(key, value);
      }
    } catch (Exception e) {
      logger.error("Error saving registered executors", e);
    }
    executors.put(fullId, executorInfo);

  }

  /**
   * Obtains a FileSegmentManagedBuffer from a single block (shuffleId, mapId, reduceId).
   */
  public ManagedBuffer getBlockData(
      String appId,
      String execId,
      int shuffleId,
      long mapId,
      int reduceId) {
    return getContinuousBlocksData(appId, execId, shuffleId, mapId, reduceId, reduceId + 1);
  }

  /**
   * Obtains a FileSegmentManagedBuffer from (shuffleId, mapId, [startReduceId, endReduceId)).
   * We make assumptions about how the hash and sort based shuffles store their data.
   */
  public ManagedBuffer getContinuousBlocksData(
      String appId,
      String execId,
      int shuffleId,
      long mapId,
      int startReduceId,
      int endReduceId) {
    ExecutorShuffleInfo executor = executors.get(new AppExecId(appId, execId));
    if (executor == null) {
      throw new RuntimeException(
        String.format("Executor is not registered (appId=%s, execId=%s)", appId, execId));
    }
    logger.trace("Executor: {}, shuffleId: {}, mapId: {}, startReduceId: {}, endReduceId: {}",executor,shuffleId, mapId, startReduceId, endReduceId);
    //20/11/20 01:20:39 TRACE RemoteShuffleBlockResolver: Executor: ExecutorShuffleInfo[localDirs=[/tmp/spark-dc91a625-66e8-42ba-acff-497b9acb7a45/executor-5009368c-727e-4f37-9a83-03e1ee2c4de8/blockmgr-b3fa52c3-050c-41d3-bc96-68b162ab8c41],subDirsPerLocalDir=64,
    // shuffleManager=org.apache.spark.shuffle.sort.SortShuffleManager], shuffleId: 0, mapId: 0, startReduceId: 43, endReduceId: 44
    return getSortBasedShuffleBlockData(executor, shuffleId, mapId, startReduceId, endReduceId);
  }

  public ManagedBuffer getRddBlockData(
      String appId,
      String execId,
      int rddId,
      int splitIndex) {
    ExecutorShuffleInfo executor = executors.get(new AppExecId(appId, execId));
    if (executor == null) {
      throw new RuntimeException(
        String.format("Executor is not registered (appId=%s, execId=%s)", appId, execId));
    }
    return getDiskPersistedRddBlockData(executor, rddId, splitIndex);
  }
  /**
   * Removes our metadata of all executors registered for the given application, and optionally
   * also deletes the local directories associated with the executors of that application in a
   * separate thread.
   *
   * It is not valid to call registerExecutor() for an executor with this appId after invoking
   * this method.
   */
  public void applicationRemoved(String appId, boolean cleanupLocalDirs) {
    logger.info("Application {} removed, cleanupLocalDirs = {}", appId, cleanupLocalDirs);
    Iterator<Map.Entry<AppExecId, ExecutorShuffleInfo>> it = executors.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<AppExecId, ExecutorShuffleInfo> entry = it.next();
      AppExecId fullId = entry.getKey();
      final ExecutorShuffleInfo executor = entry.getValue();

      // Only touch executors associated with the appId that was removed.
      if (appId.equals(fullId.appId)) {
        it.remove();
        if (db != null) {
          try {
            db.delete(dbAppExecKey(fullId));
          } catch (IOException e) {
            logger.error("Error deleting {} from executor state db", appId, e);
          }
        }

        if (cleanupLocalDirs) {
          logger.info("Cleaning up executor {}'s {} local dirs", fullId, executor.localDirs.length);

          // Execute the actual deletion in a different thread, as it may take some time.
          directoryCleaner.execute(() -> deleteExecutorDirs(executor.localDirs));
        }
      }
    }
  }

  /**
   * Removes all the files which cannot be served by the remote shuffle service (non-shuffle and
   * non-RDD files) in any local directories associated with the finished executor.
   */
  public void executorRemoved(String appId, String executorId) {
    logger.info("Clean up non-shuffle and non-RDD files associated with the finished executor {}",
      executorId);
    AppExecId fullId = new AppExecId(appId, executorId);
    final ExecutorShuffleInfo executor = executors.get(fullId);
    if (executor == null) {
      // Executor not registered, skip clean up of the local directories.
      logger.info("Executor is not registered (appId={}, execId={})", appId, executorId);
    } else {
      logger.info("Cleaning up non-shuffle and non-RDD files in executor {}'s {} local dirs",
        fullId, executor.localDirs.length);

      // Execute the actual deletion in a different thread, as it may take some time.
      directoryCleaner.execute(() -> deleteNonShuffleServiceServedFiles(executor.localDirs));
    }
  }

  /**
   * Removes all the shuffle files which are served by the remote shuffle service
   * in any local directories associated with the deregistered executor.
   */
  public void deregisterExecutor(String appId, String executorId) {
    logger.info("Clean up shuffle and files associated with the deregistered executor {}",
            executorId);
    AppExecId fullId = new AppExecId(appId, executorId);
    final ExecutorShuffleInfo executor = executors.get(fullId);
    if (executor == null) {
      // Executor not registered, skip clean up of the local directories.
      logger.info("Executor is not registered (appId={}, execId={})", appId, executorId);
    } else {
      //  Remove deregistered executor
      if (db != null) {
        try {
          db.delete(dbAppExecKey(fullId));
        } catch (IOException e) {
          logger.error("Error deleting {} from executor state db", appId, e);
        }
      }
      logger.info("Cleaning up shuffle and files associated with the deregistered executor {}'s {} local dirs",
              fullId, executor.localDirs.length);
      // Execute the actual deletion in a different thread, as it may take some time.
      directoryCleaner.execute(() -> deleteExecutorDirs(executor.localDirs));

      executors.remove(fullId);
      // Clear LoadingCaches
      //check if caching is enabled first else NullPointerException will occur
      if (executors.size() == 0 && lookAheadCachingEnabled) {
        /*logger.info("BlockDataCache size before removing all values:"+ String.valueOf(shuffleDataCache.size()));
        shuffleDataCache.invalidateAll();*/
        logger.info("BlockDataCache size before removing all values: "+ String.valueOf(dataCache.size()));
        dataCache.clear();
      }
    }
  }

  /**
   * Synchronously deletes each directory one at a time.
   * Should be executed in its own thread, as this may take a long time.
   */
  private void deleteExecutorDirs(String[] dirs) {
    for (String localDir : dirs) {
      try {
        JavaUtils.deleteRecursively(new File(localDir));
        logger.debug("Successfully cleaned up directory: {}", localDir);
      } catch (Exception e) {
        logger.error("Failed to delete directory: " + localDir, e);
      }
    }
  }

  /**
   * Synchronously deletes files not served by shuffle service in each directory recursively.
   * Should be executed in its own thread, as this may take a long time.
   */
  private void deleteNonShuffleServiceServedFiles(String[] dirs) {
    FilenameFilter filter = (dir, name) -> {
      // Don't delete shuffle data, shuffle index files or cached RDD files.
      return !name.endsWith(".index") && !name.endsWith(".data")
        && (!rddFetchEnabled || !name.startsWith("rdd_"));
    };

    for (String localDir : dirs) {
      try {
        JavaUtils.deleteRecursively(new File(localDir), filter);
        logger.debug("Successfully cleaned up files not served by shuffle service in directory: {}",
          localDir);
      } catch (Exception e) {
        logger.error("Failed to delete files not served by shuffle service in directory: "
          + localDir, e);
      }
    }
  }

  /**
   * Sort-based shuffle data uses an index called "shuffle_ShuffleId_MapId_0.index" into a data file
   * called "shuffle_ShuffleId_MapId_0.data". This logic is from IndexShuffleBlockResolver,
   * and the block id format is from ShuffleDataBlockId and ShuffleIndexBlockId.
   */
  private ManagedBuffer getSortBasedShuffleBlockData(
          ExecutorShuffleInfo executor, int shuffleId, long mapId, int startReduceId, int endReduceId) {
    File indexFile = ExecutorDiskUtils.getFile(executor.localDirs, executor.subDirsPerLocalDir,
            "shuffle_" + shuffleId + "_" + mapId + "_0.index");

    try {
      ShuffleIndexInformation shuffleIndexInformation = shuffleIndexCache.get(indexFile);
      ShuffleIndexRecord shuffleIndexRecord = shuffleIndexInformation.getIndex(
              startReduceId, endReduceId);
      //
      File dataFile = ExecutorDiskUtils.getFile(executor.localDirs, executor.subDirsPerLocalDir,
              "shuffle_" + shuffleId + "_" + mapId + "_0.data"); //.toString(); //String
      /*ShuffleDataInformation shuffleDataInformation = shuffleDataCache.get(dataFile);*/

      //ShuffleDataInformation shuffleDataInformation = dataCache.get(dataFile);

      //byte[] bytes = shuffleDataInformation.shuffleDataBuffer().array();//= dataCache.get(dataFile); // //new byte[(int) shuffleIndexRecord.getLength()];
      //byte[] remainingBytes = Arrays.copyOfRange(bytes, (int) shuffleIndexRecord.getOffset(), (int) shuffleIndexRecord.getOffset() + (int) shuffleIndexRecord.getLength());

      /*shuffleDataInformation.shuffleDataBuffer().clear(); //did nothing
      shuffleDataInformation.shuffleDataBuffer().put(new byte[0]); //did nothing
      try {
        shuffleDataInformation.close();
        System.gc();
      } catch (IOException e) {
        e.printStackTrace();
      }*/
      /*logger.trace("bytes size: "+ bytes.length);*/

      logger.trace("shuffleIndexRecord.getLength() size: "+ shuffleIndexRecord.getLength());

      /*-----------*/
      if (lookAheadCachingEnabled  && dataCache.size()>0) {
        /*synchronized(lock)
        {*/
        dataBlockCacheKeyToGet = dataFile.toString()+"-length-"+shuffleIndexRecord.getLength()+"-offset-"+shuffleIndexRecord.getOffset();

        logger.trace("shuffle data block requested from cache: "+ dataFile.toString()+"-length-"+shuffleIndexRecord.getLength()+"-offset-"+shuffleIndexRecord.getOffset());//dataBlockCacheKeyToGet); //dataFile

        ManagedBuffer blockDataBuffer = new NioManagedBuffer(ByteBuffer.allocate(0));
        if (dataCache.containsKey(dataBlockCacheKeyToGet)){
          blockDataBuffer = dataCache.get(dataBlockCacheKeyToGet);
          try{
            dataCache.remove(dataBlockCacheKeyToGet); //added
          }
          catch (ClassCastException e){ //java.util.LinkedHashMap$Entry cannot be cast to java.util.HashMap$TreeNode
            logger.trace("Cannot remove data block from cache.");
          }
          logger.trace("SIZE OF CACHE:"+dataCache.size());
          logger.trace("Getting data block from cache. Bytes to send: "+ blockDataBuffer.size());
          return blockDataBuffer;
        }else {
          logger.trace("Data block not found in cache.");
          return new FileSegmentManagedBuffer(
                  conf,
                  ExecutorDiskUtils.getFile(executor.localDirs, executor.subDirsPerLocalDir,
                          "shuffle_" + shuffleId + "_" + mapId + "_0.data"),
                  shuffleIndexRecord.getOffset(),
                  shuffleIndexRecord.getLength());
        }

        /*}*/

        /*-----------*/
        /*----------- if (lookAheadCachingEnabled  && shuffleDataCache.size()>0) {
        List<Object> list = new ArrayList<>();
        list.add(conf);
        list.add(dataFile);
        list.add(shuffleIndexRecord.getOffset());
        list.add(shuffleIndexRecord.getLength());
        ShufflePartitionInformation shufflePartitionInformation = shuffleDataCache.get(list);
        shuffleDataCache.invalidate(list);
        logger.trace("Getting data block from cache. Bytes to send: "+ shufflePartitionInformation.getSize());
        return shufflePartitionInformation.shuffleDataBuffer();
        //-----------*/
      }
      else{
        return new FileSegmentManagedBuffer(
                conf,
                ExecutorDiskUtils.getFile(executor.localDirs, executor.subDirsPerLocalDir,
                        "shuffle_" + shuffleId + "_" + mapId + "_0.data"),
                shuffleIndexRecord.getOffset(),
                shuffleIndexRecord.getLength());
      }

      //return new NioManagedBuffer(ByteBuffer.wrap(bytes));
      // NettyManagedBuffer(Unpooled.wrappedBuffer(bytes).retain());
      //return new NioManagedBuffer(buf);
      //return new NioManagedBuffer(ByteBuffer.wrap(remainingBytes));

      /*return new FileSegmentManagedBuffer(
        conf,
        ExecutorDiskUtils.getFile(executor.localDirs, executor.subDirsPerLocalDir,
          "shuffle_" + shuffleId + "_" + mapId + "_0.data"),
        shuffleIndexRecord.getOffset(),
        shuffleIndexRecord.getLength());
      */

    } catch (ExecutionException e) {
      throw new RuntimeException("Failed to open file: " + indexFile, e);
    }
  }


  /**
   * Retrieves only needed data block and stores it in cache.
   * Sort-based shuffle data uses an index called "shuffle_ShuffleId_MapId_0.index" into a data file
   * called "shuffle_ShuffleId_MapId_0.data". This logic is from IndexShuffleBlockResolver,
   * and the block id format is from ShuffleDataBlockId and ShuffleIndexBlockId.
   */
  protected ManagedBuffer retrieveBlockDataAndStoreInCache(
          String appId, String execId, int shuffleId, long mapId, int startReduceId, int endReduceId) {

    ExecutorShuffleInfo executor = executors.get(new AppExecId(appId, execId));
    if (executor == null) {
      throw new RuntimeException(
              String.format("Executor is not registered (appId=%s, execId=%s)", appId, execId));
    }
    logger.trace("Executor: {}, shuffleId: {}, mapId: {}, startReduceId: {}, endReduceId: {}",executor, shuffleId, mapId, startReduceId, endReduceId);

    File indexFile = ExecutorDiskUtils.getFile(executor.localDirs, executor.subDirsPerLocalDir,
            "shuffle_" + shuffleId + "_" + mapId + "_0.index");

    try {
      ShuffleIndexInformation shuffleIndexInformation = shuffleIndexCache.get(indexFile);
      ShuffleIndexRecord shuffleIndexRecord = shuffleIndexInformation.getIndex(
              startReduceId, endReduceId);
      //
      File dataFile = ExecutorDiskUtils.getFile(executor.localDirs, executor.subDirsPerLocalDir,
              "shuffle_" + shuffleId + "_" + mapId + "_0.data"); //.toString(); //String
      /*ShuffleDataInformation shuffleDataInformation = shuffleDataCache.get(dataFile);*/

      /*-----------*/
      /*synchronized(lock)
      {*/
      dataBlockCacheKeyToPut = dataFile.toString()+"-length-"+shuffleIndexRecord.getLength()+"-offset-"+shuffleIndexRecord.getOffset();

      ManagedBuffer bufToStoreInCache = new FileSegmentManagedBuffer(
              conf,
              ExecutorDiskUtils.getFile(executor.localDirs, executor.subDirsPerLocalDir,
                      "shuffle_" + shuffleId + "_" + mapId + "_0.data"),
              shuffleIndexRecord.getOffset(),
              shuffleIndexRecord.getLength());

      /*FileChannel channel = new RandomAccessFile(ExecutorDiskUtils.getFile(executor.localDirs, executor.subDirsPerLocalDir,
                "shuffle_" + shuffleId + "_" + mapId + "_0.data"), "r").getChannel();
      ByteBuffer buf = ByteBuffer.allocate((int) shuffleIndexRecord.getLength());
      channel.position(shuffleIndexRecord.getOffset());
      while (buf.remaining() != 0) {
        if (channel.read(buf) == -1) {
          throw new IOException(String.format("Reached EOF before filling buffer\n" +
                          "offset=%s\nfile=%s\nbuf.remaining=%s",
                  shuffleIndexRecord.getOffset(), ExecutorDiskUtils.getFile(executor.localDirs, executor.subDirsPerLocalDir,
                          "shuffle_" + shuffleId + "_" + mapId + "_0.data").getAbsoluteFile(), buf.remaining()));
        }
      }
      buf.flip();
      //return buf;
      ManagedBuffer bufToStoreInCache = new NioManagedBuffer(buf);*/


      // if (!dataCache.containsKey(dataBlockCacheKeyToPut)){
      try{
        //dataCache.put(dataFile.toString()+"-length-"+shuffleIndexRecord.getLength()+"-offset-"+shuffleIndexRecord.getOffset(),bufToStoreInCache);
        dataCache.put(dataBlockCacheKeyToPut,bufToStoreInCache);
        logger.trace("SIZE OF CACHE:"+dataCache.size());
        //test.add((new StringBuilder()).append(dataFile.toString()).append("-length-").append(shuffleIndexRecord.getLength()).append("-offset-").append(shuffleIndexRecord.getOffset()).toString().hashCode());
        //--test.add((new StringBuilder(dataFile.toString()+"-length-"+shuffleIndexRecord.getLength()+"-offset-"+shuffleIndexRecord.getOffset())).hashCode());
        //-logger.trace("SIZE OF TEST ARRAY:"+test.size()+" - SIZE OF UNIQUE HASHSETS:"+ (int) test.stream().distinct().count());
        logger.trace("Added data block in cache of Byte size: " + bufToStoreInCache.size());
      }
      catch (Exception e){
        logger.warn("Error adding data in cache.");
      }
      //}
      //logger.trace("shuffleIndexRecord.getLength() size to store: "+ shuffleIndexRecord.getLength());
      logger.trace("shuffle data block requested to store: "+ dataFile.toString()+"-length-"+shuffleIndexRecord.getLength()+"-offset-"+shuffleIndexRecord.getOffset());//dataBlockCacheKeyToPut);

      /*}*/

      /*-----------*/
      /*--------List<Object> list = new ArrayList<>();
      list.add(conf);
      list.add(dataFile);
      list.add(shuffleIndexRecord.getOffset());
      list.add(shuffleIndexRecord.getLength());
      shuffleDataCache.put(list, new ShufflePartitionInformation(list));
      ------------*/

      //byte[] bytes = shuffleDataInformation.shuffleDataBuffer().array();//= dataCache.get(dataFile); // //new byte[(int) shuffleIndexRecord.getLength()];
      //byte[] remainingBytes = Arrays.copyOfRange(bytes, (int) shuffleIndexRecord.getOffset(), (int) shuffleIndexRecord.getOffset() + (int) shuffleIndexRecord.getLength());

      return new NioManagedBuffer(ByteBuffer.allocate(0));

    } catch (ExecutionException e){ // | IOException e) {
      throw new RuntimeException("Failed to open file: " + indexFile, e);
    }
  }










  public ManagedBuffer getDiskPersistedRddBlockData(
      ExecutorShuffleInfo executor, int rddId, int splitIndex) {
    File file = ExecutorDiskUtils.getFile(executor.localDirs, executor.subDirsPerLocalDir,
      "rdd_" + rddId + "_" + splitIndex);
    long fileLength = file.length();
    ManagedBuffer res = null;
    if (file.exists()) {
      res = new FileSegmentManagedBuffer(conf, file, 0, fileLength);
    }
    return res;
  }

  void close() {
    if (db != null) {
      try {
        db.close();
      } catch (IOException e) {
        logger.error("Exception closing leveldb with registered executors", e);
      }
    }
  }

  public int removeBlocks(String appId, String execId, String[] blockIds) {
    ExecutorShuffleInfo executor = executors.get(new AppExecId(appId, execId));
    if (executor == null) {
      throw new RuntimeException(
        String.format("Executor is not registered (appId=%s, execId=%s)", appId, execId));
    }
    int numRemovedBlocks = 0;
    for (String blockId : blockIds) {
      File file =
        ExecutorDiskUtils.getFile(executor.localDirs, executor.subDirsPerLocalDir, blockId);
      if (file.delete()) {
        numRemovedBlocks++;
      } else {
        logger.warn("Failed to delete block: " + file.getAbsolutePath());
      }
    }
    return numRemovedBlocks;
  }

  public Map<String, String[]> getLocalDirs(String appId, String[] execIds) {
    return Arrays.stream(execIds)
      .map(exec -> {
        ExecutorShuffleInfo info = executors.get(new AppExecId(appId, exec));
        if (info == null) {
          throw new RuntimeException(
            String.format("Executor is not registered (appId=%s, execId=%s)", appId, exec));
        }
        return Pair.of(exec, info.localDirs);
      })
      .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  /** Simply encodes an executor's full ID, which is appId + execId. */
  public static class AppExecId {
    public final String appId;
    public final String execId;

    @JsonCreator
    public AppExecId(@JsonProperty("appId") String appId, @JsonProperty("execId") String execId) {
      this.appId = appId;
      this.execId = execId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      AppExecId appExecId = (AppExecId) o;
      return Objects.equals(appId, appExecId.appId) && Objects.equals(execId, appExecId.execId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(appId, execId);
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
        .append("appId", appId)
        .append("execId", execId)
        .toString();
    }
  }

  private static byte[] dbAppExecKey(AppExecId appExecId) throws IOException {
    // we stick a common prefix on all the keys so we can find them in the DB
    String appExecJson = mapper.writeValueAsString(appExecId);
    String key = (APP_KEY_PREFIX + ";" + appExecJson);
    return key.getBytes(StandardCharsets.UTF_8);
  }

  private static AppExecId parseDbAppExecKey(String s) throws IOException {
    if (!s.startsWith(APP_KEY_PREFIX)) {
      throw new IllegalArgumentException("expected a string starting with " + APP_KEY_PREFIX);
    }
    String json = s.substring(APP_KEY_PREFIX.length() + 1);
    AppExecId parsed = mapper.readValue(json, AppExecId.class);
    return parsed;
  }

  @VisibleForTesting
  static ConcurrentMap<AppExecId, ExecutorShuffleInfo> reloadRegisteredExecutors(DB db)
      throws IOException {
    ConcurrentMap<AppExecId, ExecutorShuffleInfo> registeredExecutors = Maps.newConcurrentMap();
    if (db != null) {
      DBIterator itr = db.iterator();
      itr.seek(APP_KEY_PREFIX.getBytes(StandardCharsets.UTF_8));
      while (itr.hasNext()) {
        Map.Entry<byte[], byte[]> e = itr.next();
        String key = new String(e.getKey(), StandardCharsets.UTF_8);
        if (!key.startsWith(APP_KEY_PREFIX)) {
          break;
        }
        AppExecId id = parseDbAppExecKey(key);
        logger.info("Reloading registered executors: " +  id.toString());
        ExecutorShuffleInfo shuffleInfo = mapper.readValue(e.getValue(), ExecutorShuffleInfo.class);
        registeredExecutors.put(id, shuffleInfo);
      }
    }
    return registeredExecutors;
  }
}