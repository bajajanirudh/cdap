/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.support.job;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.SupportBundle;
import io.cdap.cdap.support.SupportBundleState;
import io.cdap.cdap.support.lib.SupportBundleFileNames;
import io.cdap.cdap.support.status.CollectionState;
import io.cdap.cdap.support.status.SupportBundleStatus;
import io.cdap.cdap.support.status.SupportBundleTaskStatus;
import io.cdap.cdap.support.task.SupportBundleTask;
import io.cdap.cdap.support.task.factory.SupportBundleTaskFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Support bundle job to parallel process the support bundle tasks, store file to local storage and setup timeout for
 * executor.
 */
public class SupportBundleJob {

  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleJob.class);
  private static final Gson gson = new GsonBuilder().create();
  private final ExecutorService executor;
  private final SupportBundleStatus supportBundleStatus;
  private final Set<SupportBundleTaskFactory> supportBundleTaskFactories;
  private final List<SupportBundleTask> supportBundleTasks;
  private final int maxRetries;
  private final int maxThreadTimeout;
  private final Queue<RunningTaskState> runningTaskStateQueue;

  public SupportBundleJob(Set<SupportBundleTaskFactory> supportBundleTaskFactories, ExecutorService executor,
                          CConfiguration cConf, SupportBundleStatus supportBundleStatus) {
    this.supportBundleStatus = supportBundleStatus;
    this.supportBundleTaskFactories = supportBundleTaskFactories;
    this.supportBundleTasks = new ArrayList<>();
    this.executor = executor;
    this.maxRetries = cConf.getInt(Constants.SupportBundle.MAX_RETRY_TIMES);
    this.maxThreadTimeout = cConf.getInt(SupportBundle.MAX_THREAD_TIMEOUT);
    this.runningTaskStateQueue = new ConcurrentLinkedQueue<>();
  }

  /**
   * parallel processing tasks and generate support bundle
   */
  public void generateBundle(SupportBundleState supportBundleState) {
    try {
      String basePath = supportBundleState.getBasePath();
      supportBundleTasks.addAll(supportBundleTaskFactories.stream().map(factory -> factory.create(supportBundleState))
                                  .collect(Collectors.toList()));
      for (SupportBundleTask supportBundleTask : supportBundleTasks) {
        String className = supportBundleTask.getClass().getName();
        String taskName = supportBundleState.getUuid().concat(": ").concat(className);
        SupportBundleTaskStatus taskStatus = initializeTask(taskName, className);
        executeTask(taskStatus, supportBundleTask, basePath, className, taskName, 0);
      }
      completeProcessing(basePath);
    } catch (Exception e) {
      LOG.warn("Failed to execute the tasks ", e);
    }
  }

  /**
   * Execute each task to generate support bundle files
   */
  public void executeTask(SupportBundleTaskStatus taskStatus, SupportBundleTask supportBundleTask, String basePath,
                          String className, String taskName, int retryCount) {
    List<Long> startTimeStore = new ArrayList<>(1);
    Stack<SupportBundleTaskStatus> statusStack = new Stack<>();
    statusStack.add(taskStatus);
    Future<SupportBundleTaskStatus> futureService = executor.submit(() -> {
      try {
        long startTime = System.currentTimeMillis();
        startTimeStore.add(startTime);
        statusStack.add(updateTask(statusStack.peek(), basePath, CollectionState.IN_PROGRESS));
        supportBundleTask.collect();
        statusStack.add(updateTask(statusStack.peek(), basePath, CollectionState.FINISHED));
      } catch (Exception e) {
        LOG.warn("Failed to execute task with supportBundleTask {} ", taskName, e);
        executeTaskAgainAfterFailed(supportBundleTask, className, taskName, statusStack.peek(), basePath,
                                    retryCount + 1);
      }
      return statusStack.peek();
    });
    RunningTaskState runningTaskState = new RunningTaskState(futureService, startTimeStore.get(0), taskStatus);
    runningTaskStateQueue.offer(runningTaskState);
  }

  /**
   * Execute all processing
   */
  public void completeProcessing(String basePath) {
    while (!runningTaskStateQueue.isEmpty()) {
      RunningTaskState runningTaskState = runningTaskStateQueue.poll();
      Future<SupportBundleTaskStatus> future = runningTaskState.getFuture();
      try {
        long currentTime = System.currentTimeMillis();
        long futureStartTime = runningTaskState.getStartTime();
        long timeLeftBeforeTimeout = TimeUnit.MINUTES.toMillis(maxThreadTimeout) - (currentTime - futureStartTime);
        future.get(timeLeftBeforeTimeout, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        LOG.error("The task for has failed or timeout more than five minutes ", e);
        updateFailedTask(runningTaskState.getTaskStatus(), future, basePath);
      }
    }
  }

  /**
   * Start a new status task
   */
  public SupportBundleTaskStatus initializeTask(String name, String type) {
    SupportBundleTaskStatus supportBundleTaskStatus =
      new SupportBundleTaskStatus(name, type, System.currentTimeMillis());
    supportBundleStatus.getTasks().add(supportBundleTaskStatus);
    return supportBundleTaskStatus;
  }

  /**
   * Update status task
   */
  private SupportBundleTaskStatus updateTask(SupportBundleTaskStatus taskStatus, String basePath,
                                             CollectionState status) {
    SupportBundleTaskStatus newTaskStatus = new SupportBundleTaskStatus(taskStatus, System.currentTimeMillis(), status);
    supportBundleStatus.getTasks().remove(taskStatus);
    supportBundleStatus.getTasks().add(newTaskStatus);
    addToStatus(basePath);
    return newTaskStatus;
  }

  /**
   * Update status file
   */
  private void addToStatus(String basePath) {
    try (FileWriter statusFile = new FileWriter(new File(basePath, SupportBundleFileNames.statusFileName))) {
      gson.toJson(supportBundleStatus, statusFile);
    } catch (IOException e) {
      LOG.error("Failed to update status file ", e);
    }
  }

  /**
   * Queue the task again after exception
   */
  private void executeTaskAgainAfterFailed(SupportBundleTask supportBundleTask, String className, String taskName,
                                           SupportBundleTaskStatus taskStatus, String basePath, int retryCount) {
    if (retryCount >= maxRetries) {
      LOG.error("The task has reached maximum times of retries {} ", taskName);
      updateTask(taskStatus, basePath, CollectionState.FAILED);
    } else {
      SupportBundleTaskStatus updatedTaskStatus =
        new SupportBundleTaskStatus(taskStatus, retryCount, CollectionState.QUEUED);
      supportBundleStatus.getTasks().remove(taskStatus);
      supportBundleStatus.getTasks().add(updatedTaskStatus);
      addToStatus(basePath);
      executeTask(taskStatus, supportBundleTask, basePath, className, taskName, retryCount);
    }
  }

  /**
   * Update failed status
   */
  private void updateFailedTask(SupportBundleTaskStatus supportBundleTaskStatus, Future<SupportBundleTaskStatus> future,
                                String basePath) {
    LOG.error("The task for has failed or timeout more than five minutes ");
    future.cancel(true);
    if (supportBundleTaskStatus != null) {
      updateTask(supportBundleTaskStatus, basePath, CollectionState.FAILED);
    }
  }
}
