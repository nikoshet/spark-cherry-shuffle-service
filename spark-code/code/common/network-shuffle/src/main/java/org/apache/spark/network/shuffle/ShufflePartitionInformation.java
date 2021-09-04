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

import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.util.TransportConf;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Keeps the data information for a particular map output
 * as an in-memory LongBuffer.
 */
public class ShufflePartitionInformation extends InputStream {
  /** offsets as long buffer */
  private final ManagedBuffer buffer;
  private final int size;


  public ShufflePartitionInformation(List<Object> list) throws IOException {

    //list obj1: TransportConf
    //list obj2: String
    //list obj3: Long
    //list obj4: Long

    TransportConf conf = (TransportConf) list.get(0);
    File dataFile = (File) list.get(1);
    long offset = (long) list.get(2);
    long length = (long) list.get(3);

    /*size = (int)dataFile.length();
    buffer = ByteBuffer.allocate(size);

    String t = "a";
    TransportConf conf = t.getBytes(StandardCharsets.UTF_8);
    try {
      DataInputStream dis = new DataInputStream(Files.newInputStream(dataFile.toPath()));
      dis.readFully(buffer.array()); //readFully(buffer.array(), 0, size); //
      dis.close();
    } catch (IOException e) {
      e.printStackTrace();
    }*/

    buffer = new FileSegmentManagedBuffer(
            conf,
            dataFile,
            offset,
            length); //.nioByteBuffer();
    //size = buffer.array().length;
    size = (int)buffer.size();

  }

  /**
   * Get block data in a buffer
   * @return buffered data
   */
  /*public DataInputStream getBufferedData(File dataFile) throws IOException  {
    size = (int)dataFile.length();
    ByteBuffer buffer = ByteBuffer.allocate(size);
    //offsets = buffer.asLongBuffer();
    try (DataInputStream dis = new DataInputStream(Files.newInputStream(dataFile.toPath()))) {
      dis.readFully(buffer.array());
      return dis;
    }
    catch(Exception e){
      return null;
    }
  }*/


  /**
   * Size of the data file
   * @return size
   */
  public int getSize() {
    return size;
  }

  @Override
  public int read() throws IOException {
    return 0;
  }

  /**
   * Get index offset for a particular reducer.
   */
  /*public ShuffleIndexRecord getIndex(int reduceId) {
    return getIndex(reduceId, reduceId + 1);
  }*/

  /**
   * Get shuffle data buffer.
   */
  public ManagedBuffer shuffleDataBuffer() {
    return buffer;
  }
}
