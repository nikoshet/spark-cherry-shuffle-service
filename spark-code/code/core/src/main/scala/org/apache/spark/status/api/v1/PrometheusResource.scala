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
package org.apache.spark.status.api.v1

import javax.ws.rs._
import javax.ws.rs.core.MediaType
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.glassfish.jersey.server.ServerProperties
import org.glassfish.jersey.servlet.ServletContainer
import org.apache.spark.{SPARK_REVISION, SPARK_VERSION_SHORT, SparkContext}
import org.apache.spark.annotation.Experimental
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.{Clock, SystemClock}

/**
 * :: Experimental ::
 * This aims to expose Executor metrics like REST API which is documented in
 *
 *    https://spark.apache.org/docs/3.0.0/monitoring.html#executor-metrics
 *
 * Note that this is based on ExecutorSummary which is different from ExecutorSource.
 */
@Experimental
@Path("/executors")
private[v1] class PrometheusResource extends ApiRequestContext {
  @GET
  @Path("prometheus")
  @Produces(Array(MediaType.TEXT_PLAIN))
  def executors(): String = {
    val sb = new StringBuilder
    sb.append(s"""spark_info{version="$SPARK_VERSION_SHORT", revision="$SPARK_REVISION"} 1.0\n""")
    val store = uiRoot.asInstanceOf[SparkUI].store
    store.executorList(true).foreach { executor =>
      val prefix = "metrics_executor_"
      val labels = Seq(
        "application_id" -> store.applicationInfo.id,
        "application_name" -> store.applicationInfo.name,
        "executor_id" -> executor.id
      ).map { case (k, v) => s"""$k="$v"""" }.mkString("{", ", ", "}")
      sb.append(s"${prefix}rddBlocks$labels ${executor.rddBlocks}\n")
      sb.append(s"${prefix}memoryUsed_bytes$labels ${executor.memoryUsed}\n")
      sb.append(s"${prefix}diskUsed_bytes$labels ${executor.diskUsed}\n")
      sb.append(s"${prefix}totalCores$labels ${executor.totalCores}\n")
      sb.append(s"${prefix}maxTasks$labels ${executor.maxTasks}\n")
      sb.append(s"${prefix}activeTasks$labels ${executor.activeTasks}\n")
      sb.append(s"${prefix}failedTasks_total$labels ${executor.failedTasks}\n")
      sb.append(s"${prefix}completedTasks_total$labels ${executor.completedTasks}\n")
      sb.append(s"${prefix}totalTasks_total$labels ${executor.totalTasks}\n")
      sb.append(s"${prefix}totalDuration_seconds_total$labels ${executor.totalDuration * 0.001}\n")
      sb.append(s"${prefix}totalGCTime_seconds_total$labels ${executor.totalGCTime * 0.001}\n")
      sb.append(s"${prefix}totalInputBytes_bytes_total$labels ${executor.totalInputBytes}\n")
      sb.append(s"${prefix}totalShuffleRead_bytes_total$labels ${executor.totalShuffleRead}\n")
      sb.append(s"${prefix}totalShuffleWrite_bytes_total$labels ${executor.totalShuffleWrite}\n")
      sb.append(s"${prefix}maxMemory_bytes$labels ${executor.maxMemory}\n")
      executor.executorLogs.foreach { case (k, v) => }
      executor.memoryMetrics.foreach { m =>
        sb.append(s"${prefix}usedOnHeapStorageMemory_bytes$labels ${m.usedOnHeapStorageMemory}\n")
        sb.append(s"${prefix}usedOffHeapStorageMemory_bytes$labels ${m.usedOffHeapStorageMemory}\n")
        sb.append(s"${prefix}totalOnHeapStorageMemory_bytes$labels ${m.totalOnHeapStorageMemory}\n")
        sb.append(s"${prefix}totalOffHeapStorageMemory_bytes$labels " +
          s"${m.totalOffHeapStorageMemory}\n")
      }

      executor.peakMemoryMetrics.foreach { m =>
        val names = Array(
          "JVMHeapMemory",
          "JVMOffHeapMemory",
          "OnHeapExecutionMemory",
          "OffHeapExecutionMemory",
          "OnHeapStorageMemory",
          "OffHeapStorageMemory",
          "OnHeapUnifiedMemory",
          "OffHeapUnifiedMemory",
          "DirectPoolMemory",
          "MappedPoolMemory",
          "ProcessTreeJVMVMemory",
          "ProcessTreeJVMRSSMemory",
          "ProcessTreePythonVMemory",
          "ProcessTreePythonRSSMemory",
          "ProcessTreeOtherVMemory",
          "ProcessTreeOtherRSSMemory"
        )
        names.foreach { name =>
          sb.append(s"$prefix${name}_bytes$labels ${m.getMetricValue(name)}\n")
        }
        Seq("MinorGCCount", "MajorGCCount").foreach { name =>
          sb.append(s"$prefix${name}_total$labels ${m.getMetricValue(name)}\n")
        }
        Seq("MinorGCTime", "MajorGCTime").foreach { name =>
          sb.append(s"$prefix${name}_seconds_total$labels ${m.getMetricValue(name) * 0.001}\n")
        }
      }
    }

    //******** Added Spark Metrics For Prometheus Scraping ********//
    val prefix = "spark_metrics_"
    //--- General metrics ---//
    val simpleLabels = Seq(
      "application_id" -> store.applicationInfo().id,
      "application_name" -> store.applicationInfo().name,
    ).map { case (k, v) => s"""$k="$v"""" }.mkString("{", ", ", "}")

    sb.append(s"${prefix}number_of_waiting_stages_of_job$simpleLabels ${SparkContext.getOrCreate().dagScheduler.waitingStages.size}\n")
    sb.append(s"${prefix}number_of_running_stages_of_job$simpleLabels ${SparkContext.getOrCreate().dagScheduler.runningStages.size}\n")

    //--- Metrics for running stages ---//
    SparkContext.getOrCreate().dagScheduler.runningStages.foreach { runningStage =>
      val customLabels = Seq(
        "application_id" -> store.applicationInfo().id,
        "application_name" -> store.applicationInfo().name,
        "running_stage_id" -> runningStage.id
      ).map { case (k, v) => s"""$k="$v"""" }.mkString("{", ", ", "}")

      sb.append(s"${prefix}running_stage_numTasks$customLabels ${runningStage.numTasks}\n")
      //sb.append(s"${prefix}running_stage_id$labels ${runningStage.id}\n")
      //sb.append(s"${prefix}running_stage_name$customLabels ${runningStage.latestInfo.name}\n")
      val clock: Clock = new SystemClock()
      val runningTime = runningStage.latestInfo.submissionTime match {
        case Some(t) => "%.03f".format((clock.getTimeMillis() - t) / 1000.0)
        case _ => 0
      }
      sb.append(s"${prefix}running_stage_running_time_sec$customLabels ${runningTime}\n")

      store.stageData(runningStage.id).foreach { stageData =>
        sb.append(s"${prefix}running_stage_stageData_complete_tasks$customLabels ${stageData.numCompleteTasks}\n")
        // it also has info about shuffle read/write size etc

        /*store.stageData(runningStage.id, true).foreach { s =>
          s.tasks.foreach { t =>
            t.get(0).foreach { t =>
            }
          }
        }*/
        /* .foreach { taskMap =>
          taskMap.toMap.
          task.map(_.shuffleReadMetrics.fetchWaitTime)
          sb.append(s"${prefix}running_stage_task_id$customLabels ${task.}\n")
        }*/
      /*  sb.append(s"${prefix}running_stage_task_shuffle_read_time_TEST 1 \n")
        sb.append(s"${prefix}running_stage_task_shuffle_stagedataTasks_TEST ${Some(stageData.tasks)} \n")
        //stageData.tasks.map (_.foreach { t =>
        stageData.tasks.foreach{ t =>


          //case (l: Long, taskData: TaskData)  =>
            //stageData.tasks.get.foreach { t =>
          val taskData = t.values //._2
          taskData.foreach { taskData =>

            val customTaskLabels = Seq(
              "application_id" -> store.applicationInfo().id,
              "application_name" -> store.applicationInfo().name,
              "running_stage_id" -> runningStage.id,
              "task_id" -> taskData.taskId
            ).map { case (k, v) => s"""$k="$v"""" }.mkString("{", ", ", "}")

            taskData.taskMetrics.map(_.shuffleReadMetrics.fetchWaitTime).getOrElse(0L)

            sb.append(s"${prefix}running_stage_task_shuffle_read_time$customTaskLabels 2 \n")
            // shuffle read metrics per task
            val shuffleReadTime = taskData.taskMetrics.map(_.shuffleReadMetrics.fetchWaitTime).getOrElse(0L)
            val remoteBlocksFetched = taskData.taskMetrics.map(_.shuffleReadMetrics.remoteBlocksFetched).getOrElse(0L)
            val remoteBytesReadToDisk = taskData.taskMetrics.map(_.shuffleReadMetrics.remoteBytesReadToDisk).getOrElse(0L)
            val remoteBytesRead = taskData.taskMetrics.map(_.shuffleReadMetrics.remoteBytesRead).getOrElse(0L)
            val localBytesRead = taskData.taskMetrics.map(_.shuffleReadMetrics.localBytesRead).getOrElse(0L)
            val localBlocksFetched = taskData.taskMetrics.map(_.shuffleReadMetrics.localBlocksFetched).getOrElse(0L)
            val recordsRead = taskData.taskMetrics.map(_.shuffleReadMetrics.recordsRead).getOrElse(0L)


            //sb.append(s"${prefix}running_stage_task_shuffle_read_time$customTaskLabels ${taskData.taskMetrics.map ( _.shuffleReadMetrics.fetchWaitTime)}\n")
            // sb.append(s"${prefix}running_stage_task_shuffle_read_remote_bytes_read$customTaskLabels ${t._2.taskMetrics.get.shuffleReadMetrics.remoteBytesRead}\n")
            //sb.append(s"${prefix}running_stage_task_shuffle_read_remote_bytes_read$customTaskLabels ${taskData.taskMetrics.map ( { case (metrics: TaskMetrics)  => metrics.shuffleReadMetrics.remoteBytesRead})}\n")

            sb.append(s"${prefix}running_stage_task_shuffleReadTime$customTaskLabels $shuffleReadTime\n")
            sb.append(s"${prefix}running_stage_task_remoteBlocksFetched$customTaskLabels $remoteBlocksFetched\n")
            sb.append(s"${prefix}running_stage_task_remoteBytesReadToDisk$customTaskLabels $remoteBytesReadToDisk\n")
            sb.append(s"${prefix}running_stage_task_remoteBytesRead$customTaskLabels $remoteBytesRead\n")
            sb.append(s"${prefix}running_stage_task_localBytesRead$customTaskLabels $localBytesRead\n")
            sb.append(s"${prefix}running_stage_task_localBlocksFetched$customTaskLabels $localBlocksFetched\n")
            sb.append(s"${prefix}running_stage_task_recordsRead$customTaskLabels $recordsRead\n")

            // shuffle write metrics per task
            val recordsWritten = taskData.taskMetrics.map(_.shuffleWriteMetrics.recordsWritten).getOrElse(0L)
            val bytesWritten = taskData.taskMetrics.map(_.shuffleWriteMetrics.bytesWritten).getOrElse(0L)
            val writeTime = taskData.taskMetrics.map(_.shuffleWriteMetrics.writeTime).getOrElse(0L)

            //sb.append(s"${prefix}running_stage_task_shuffle_write_time$customTaskLabels ${taskData.taskMetrics.map ( { case (metrics: TaskMetrics)  => metrics.shuffleWriteMetrics.writeTime})}\n")
            //sb.append(s"${prefix}running_stage_task_shuffle_bytes_written$customTaskLabels ${taskData.taskMetrics.map ( { case (metrics: TaskMetrics)  => metrics.shuffleWriteMetrics.bytesWritten})}\n")
            //sb.append(s"${prefix}running_stage_task_shuffle_records_written$customTaskLabels ${taskData.taskMetrics.map ( { case (metrics: TaskMetrics)  => metrics.shuffleWriteMetrics.recordsWritten})}\n")

            sb.append(s"${prefix}running_stage_task_recordsWritten$customTaskLabels $recordsWritten\n")
            sb.append(s"${prefix}running_stage_task_bytesWritten$customTaskLabels $bytesWritten\n")
            sb.append(s"${prefix}running_stage_task_writeTime$customTaskLabels $writeTime\n")
          }
        }//)
        */

      }
    }

    //--- Metrics for waiting stages ---//
    SparkContext.getOrCreate().dagScheduler.waitingStages.foreach { waitingStage =>
      val customLabels = Seq(
        "application_id" -> store.applicationInfo().id,
        "application_name" -> store.applicationInfo().name,
        "waiting_stage_id" -> waitingStage.id
      ).map { case (k, v) => s"""$k="$v"""" }.mkString("{", ", ", "}")
      sb.append(s"${prefix}waiting_stage_numTasks$customLabels ${waitingStage.numTasks}\n")
    }

    //store testing
    // DagScheduler: variable numTotalJobs line 139
    //store.activeStages().foreach {
    //}
    //.schedulerBackend. ._schedulerBackend. .manager.taskSet.tasks.length} \n")
    //sb.append(s"${prefix}TESTING_stages_of_job$labels ${store.activeStages().stageId}\n")
    //******** ---------------------- ********//

    sb.toString
  }
}

private[spark] object PrometheusResource {
  def getServletHandler(uiRoot: UIRoot): ServletContextHandler = {
    val jerseyContext = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
    jerseyContext.setContextPath("/metrics")
    val holder: ServletHolder = new ServletHolder(classOf[ServletContainer])
    holder.setInitParameter(ServerProperties.PROVIDER_PACKAGES, "org.apache.spark.status.api.v1")
    UIRootFromServletContext.setUiRoot(jerseyContext, uiRoot)
    jerseyContext.addServlet(holder, "/*")
    jerseyContext
  }
}
