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

package org.apache.spark.scheduler

import scala.collection.mutable.HashSet

import org.apache.spark.ShuffleDependency
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.CallSite

/**
 * ShuffleMapStages are intermediate stages in the execution DAG that produce data for a shuffle.
 * They occur right before each shuffle operation, and might contain multiple pipelined operations
 * before that (e.g. map and filter). When executed, they save map output files that can later be
 * fetched by reduce tasks. The `shuffleDep` field describes the shuffle each stage is part of,
 * and variables like `outputLocs` and `numAvailableOutputs` track how many map outputs are ready.
 *
 * ShuffleMapStages can also be submitted independently as jobs with DAGScheduler.submitMapStage.
 * For such stages, the ActiveJobs that submitted them are tracked in `mapStageJobs`. Note that
 * there can be multiple ActiveJobs trying to compute the same shuffle map stage.
 */
private[spark] class ShuffleMapStage(
    id: Int,
    rdd: RDD[_],
    numTasks: Int,
    parents: List[Stage],
    firstJobId: Int,
    callSite: CallSite,
    val shuffleDep: ShuffleDependency[_, _, _]) // 与ShuffleMapStage相对于的ShuffleDependency
  extends Stage(id, rdd, numTasks, parents, firstJobId, callSite) {

  private[this] var _mapStageJobs: List[ActiveJob] = Nil // 与ShuffleMapStage相对应的ActiveJob的列表

  private[this] var _numAvailableOutputs: Int = 0 // ShuffleMapStage可用的map任务的输出数量，这也代表了执行成功的map任务数

  /**
   * Partitions that either haven't yet been computed, or that were computed on an executor
   * that has since been lost, so should be re-computed.  This variable is used by the
   * DAGScheduler to determine when a stage has completed. Task successes in both the active
   * attempt for the stage or in earlier attempts for this stage can cause paritition ids to get
   * removed from pendingPartitions. As a result, this variable may be inconsistent with the pending
   * tasks in the TaskSetManager for the active attempt for the stage (the partitions stored here
   * will always be a subset of the partitions that the TaskSetManager thinks are pending).
   */
  val pendingPartitions = new HashSet[Int]

  /**
   * List of [[MapStatus]] for each partition. The index of the array is the map partition id,
   * and each value in the array is the list of possible [[MapStatus]] for a partition
   * (a single task might run multiple times).
   */
  // ShuffleMapStage的各个map任务与其对应的MapStatus列表的映射关系，由于map任务可能会运行多次，因而可能会有多个MapStatus
  private[this] val outputLocs = Array.fill[List[MapStatus]](numPartitions)(Nil)

  override def toString: String = "ShuffleMapStage " + id

  /**
   * Returns the list of active jobs,
   * i.e. map-stage jobs that were submitted to execute this stage independently (if any).
   */
  def mapStageJobs: Seq[ActiveJob] = _mapStageJobs // 读取_mapStageJobs

  /** Adds the job to the active job list. */
  def addActiveJob(job: ActiveJob): Unit = { // 向ShuffleMapStage相关联的ActiveJob的列表中添加ActiveJob
    _mapStageJobs = job :: _mapStageJobs
  }

  /** Removes the job from the active job list. */
  def removeActiveJob(job: ActiveJob): Unit = { // 向ShuffleMapStage相关联的ActiveJob的列表中删除ActiveJob
    _mapStageJobs = _mapStageJobs.filter(_ != job)
  }

  /**
   * Number of partitions that have shuffle outputs.
   * When this reaches [[numPartitions]], this map stage is ready.
   * This should be kept consistent as `outputLocs.filter(!_.isEmpty).size`.
   */
  def numAvailableOutputs: Int = _numAvailableOutputs // 读取_numAvailableOutputs

  /**
   * Returns true if the map stage is ready, i.e. all partitions have shuffle outputs.
   * This should be the same as `outputLocs.contains(Nil)`.
   */
  // 当_numAvailableOutputs与numPartitions相等时为true，也就是说，ShuffleMapStage的所有分区的map任务都执行成功后，ShuffleMapStage才是可用的
  def isAvailable: Boolean = _numAvailableOutputs == numPartitions

  /** Returns the sequence of partition ids that are missing (i.e. needs to be computed). */
  override def findMissingPartitions(): Seq[Int] = { // 找到所有还未执行成功而需要计算的分区
    val missing = (0 until numPartitions).filter(id => outputLocs(id).isEmpty)
    assert(missing.size == numPartitions - _numAvailableOutputs,
      s"${missing.size} missing, expected ${numPartitions - _numAvailableOutputs}")
    missing
  }

  /**
    * 当某一分区的任务执行成功后，首先将分区与MapStatus的对应关系添加到outputLocs中，然后将可用的输出数加一
    * @param partition
    * @param status
    */
  def addOutputLoc(partition: Int, status: MapStatus): Unit = {
    val prevList = outputLocs(partition)
    outputLocs(partition) = status :: prevList //首先将分区与MapStatus的对应关系添加到outputLocs中
    if (prevList == Nil) {
      _numAvailableOutputs += 1 // 然后将可用的输出数加一
    }
  }

  def removeOutputLoc(partition: Int, bmAddress: BlockManagerId): Unit = {
    val prevList = outputLocs(partition)
    val newList = prevList.filterNot(_.location == bmAddress)
    outputLocs(partition) = newList
    if (prevList != Nil && newList == Nil) {
      _numAvailableOutputs -= 1
    }
  }

  /**
   * Returns an array of [[MapStatus]] (index by partition id). For each partition, the returned
   * value contains only one (i.e. the first) [[MapStatus]]. If there is no entry for the partition,
   * that position is filled with null.
   */
  def outputLocInMapOutputTrackerFormat(): Array[MapStatus] = {
    outputLocs.map(_.headOption.orNull)
  }

  /**
   * Removes all shuffle outputs associated with this executor. Note that this will also remove
   * outputs which are served by an external shuffle server (if one exists), as they are still
   * registered with this execId.
   */
  def removeOutputsOnExecutor(execId: String): Unit = {
    var becameUnavailable = false
    for (partition <- 0 until numPartitions) {
      val prevList = outputLocs(partition)
      val newList = prevList.filterNot(_.location.executorId == execId)
      outputLocs(partition) = newList
      if (prevList != Nil && newList == Nil) {
        becameUnavailable = true
        _numAvailableOutputs -= 1
      }
    }
    if (becameUnavailable) {
      logInfo("%s is now unavailable on executor %s (%d/%d, %s)".format(
        this, execId, _numAvailableOutputs, numPartitions, isAvailable))
    }
  }
}
