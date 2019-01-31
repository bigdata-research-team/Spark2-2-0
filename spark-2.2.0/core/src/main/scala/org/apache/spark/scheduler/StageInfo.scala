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

import scala.collection.mutable.HashMap

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.storage.RDDInfo

/**
 * :: DeveloperApi ::
 * Stores information about a stage to pass from the scheduler to SparkListeners.
 */
@DeveloperApi
class StageInfo(
    val stageId: Int, // Stage的Id
    val attemptId: Int, // 当前Stage尝试的Id
    val name: String, // 当前Stage的名字
    val numTasks: Int, // 当前Stage的Task数量
    val rddInfos: Seq[RDDInfo], // RDDInfo的序列
    val parentIds: Seq[Int], // 当前Stage的父Stage的Id序列
    val details: String, // 详细的线程栈信息
    val taskMetrics: TaskMetrics = null, // Task的度量信息
    private[spark] val taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty) { // 用于存储任务的本地性偏好
  /** When this stage was submitted from the DAGScheduler to a TaskScheduler. */
  var submissionTime: Option[Long] = None // DAGScheduler将当前Stage提交给TaskScheduler的时间
  /** Time when all tasks in the stage completed or when the stage was cancelled. */
  var completionTime: Option[Long] = None // 当前Stage中所有Task完成的时间（即Stage完成的时间）或Stage被取消的时间
  /** If the stage failed, the reason why. */
  var failureReason: Option[String] = None // 如果Stage失败了，用于记录失败的原因

  /**
   * Terminal values of accumulables updated during this stage, including all the user-defined
   * accumulators.
   */
  val accumulables = HashMap[Long, AccumulableInfo]() // 存储了所有聚合器计算的最终值

  /**
    * 当一个Stage失败时要调用的方法
    * @param reason
    */
  def stageFailed(reason: String) {
    failureReason = Some(reason) // 保存失败原因
    completionTime = Some(System.currentTimeMillis) // 保存失败时间
  }

  private[spark] def getStatusString: String = {
    if (completionTime.isDefined) {
      if (failureReason.isDefined) {
        "failed"
      } else {
        "succeeded"
      }
    } else {
      "running"
    }
  }
}

private[spark] object StageInfo {
  /**
   * Construct a StageInfo from a Stage.
   *
   * Each Stage is associated with one or many RDDs, with the boundary of a Stage marked by
   * shuffle dependencies. Therefore, all ancestor RDDs related to this Stage's RDD through a
   * sequence of narrow dependencies should also be associated with this Stage.
   */
  def fromStage( // 构建StageInfo的方法
      stage: Stage,
      attemptId: Int,
      numTasks: Option[Int] = None,
      taskMetrics: TaskMetrics = null,
      taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty
    ): StageInfo = {
    // getNarrowAncestors方法获取RDD的祖先依赖中属于窄依赖的RDD序列，对获取到的RDD序列中的每一个RDD调用fromRdd方法创建RDDInfo对象
    val ancestorRddInfos = stage.rdd.getNarrowAncestors.map(RDDInfo.fromRdd)
    // 将RDDInfo放入rddInfos中
    val rddInfos = Seq(RDDInfo.fromRdd(stage.rdd)) ++ ancestorRddInfos //
    new StageInfo( // 创建StageInfo
      stage.id,
      attemptId,
      stage.name,
      numTasks.getOrElse(stage.numTasks),
      rddInfos,
      stage.parents.map(_.id),
      stage.details,
      taskMetrics,
      taskLocalityPreferences)
  }
}
