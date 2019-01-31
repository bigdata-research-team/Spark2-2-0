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

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{Future, Promise}

import org.apache.spark.internal.Logging

/**
 * An object that waits for a DAGScheduler job to complete. As tasks finish, it passes their
 * results to the given handler function.
 */
private[spark] class JobWaiter[T](
    dagScheduler: DAGScheduler, // 当前JobWaiter等待执行完成的Job的调度者
    val jobId: Int, // 当前JobWaiter等待执行完成的Job的身份标识
    totalTasks: Int, // 等待完成的Job包括的Task的数量
    resultHandler: (Int, T) => Unit) // 执行结果的处理器
  extends JobListener with Logging {

  private val finishedTasks = new AtomicInteger(0) // 等待完成的Job中已经完成的Task数量
  // If the job is finished, this will be its result. In the case of 0 task jobs (e.g. zero
  // partition RDDs), we set the jobResult directly to JobSucceeded.
  private val jobPromise: Promise[Unit] = // 用来代表Job完成后的结果，如果totalTasks为0，说明没有Task需要执行，将直接设为true
    if (totalTasks == 0) Promise.successful(()) else Promise()

  def jobFinished: Boolean = jobPromise.isCompleted // Job是否已经完成

  def completionFuture: Future[Unit] = jobPromise.future

  /**
   * Sends a signal to the DAGScheduler to cancel the job. The cancellation itself is handled
   * asynchronously. After the low level scheduler cancels all the tasks belonging to this job, it
   * will fail this job with a SparkException.
   */
  def cancel() { // 取消对Job的执行
    dagScheduler.cancelJob(jobId, None) // 调用DAGScheduler的cancelJob方法，将使得Job的所有任务被取消，并向外抛出SparkException
  }

  override def taskSucceeded(index: Int, result: Any): Unit = { // 重写JobListener特质的taskSucceeded方法
    // resultHandler call must be synchronized in case resultHandler itself is not thread safe.
    synchronized {
      resultHandler(index, result.asInstanceOf[T]) // 处理Job中每个Task的执行结果
    }
    if (finishedTasks.incrementAndGet() == totalTasks) { // 增加已完成的Task数，比与所有Task数比较，若都完成则jobPromise设为true
      jobPromise.success(())
    }
  }

  override def jobFailed(exception: Exception): Unit = {// 重写JobListener特质的jobFailed方法
    if (!jobPromise.tryFailure(exception)) { // 只是将jobPromise设置为Failure
      logWarning("Ignore failure", exception)
    }
  }

}
