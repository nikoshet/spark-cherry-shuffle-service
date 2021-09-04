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

package org.apache.spark.network.shuffle.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.spark.network.protocol.Encoders;

import java.util.Objects;

// Needed by ScalaDoc. See SPARK-7726


/**
 * Initial registration message between an executor and its local shuffle server.
 * Returns nothing (empty byte array).
 */
public class DeregisterExecutor extends BlockTransferMessage {
  public final String appId;
  public final String execId;

  public DeregisterExecutor(
      String appId,
      String execId) {
    this.appId = appId;
    this.execId = execId;
  }

  @Override
  protected Type type() { return Type.DEREGISTER_EXECUTOR; }

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

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof DeregisterExecutor) {
      DeregisterExecutor o = (DeregisterExecutor) other;
      return Objects.equals(appId, o.appId)
        && Objects.equals(execId, o.execId);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(appId)
      + Encoders.Strings.encodedLength(execId);
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, appId);
    Encoders.Strings.encode(buf, execId);
  }

  public static DeregisterExecutor decode(ByteBuf buf) {
    String appId = Encoders.Strings.decode(buf);
    String execId = Encoders.Strings.decode(buf);
    return new DeregisterExecutor(appId, execId);
  }
}
