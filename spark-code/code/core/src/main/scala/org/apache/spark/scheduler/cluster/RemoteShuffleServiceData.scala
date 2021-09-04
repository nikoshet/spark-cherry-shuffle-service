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

package org.apache.spark.scheduler.cluster

import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef}
import org.apache.spark.scheduler.ExecutorResourceInfo

/**
 * Grouping of data for a RemoteShuffleService used by CoarseGrainedSchedulerBackend.
 *
 * @param rssEndpoint The RpcEndpointRef representing this executor
 * @param rssAddress The network address of this executor
 * @param rssHost The hostname that this executor is running on
 */
private[cluster] class RemoteShuffleServiceData(
    val rssEndpoint: RpcEndpointRef,
    val rssAddress: RpcAddress,
    val rssHost: String
)
