/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker.system;

import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.ServiceListenerAdapter;

import java.util.concurrent.CompletableFuture;

public class SystemWorkerTestUtil {

  /**
   * Creates a listener for the task worker service that updates a {@link CompletableFuture < Service.State>}
   *
   * @param systemWorker {@link SystemWorkerService}
   */
  static CompletableFuture<Service.State> getServiceCompletionFuture(SystemWorkerService systemWorker) {
    CompletableFuture<Service.State> future = new CompletableFuture<>();
    systemWorker.addListener(new ServiceListenerAdapter() {
      @Override
      public void terminated(Service.State from) {
        future.complete(from);
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        future.completeExceptionally(failure);
      }
    }, Threads.SAME_THREAD_EXECUTOR);
    return future;
  }

  static void waitForServiceCompletion(CompletableFuture<Service.State> future) {
    try {
      Uninterruptibles.getUninterruptibly(future);
    } catch (Exception e) {
      //ignore
    }
  }

}
