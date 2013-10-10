package com.continuuity.internal.app.runtime.schedule;

import org.quartz.SchedulerConfigException;
import org.quartz.spi.ThreadPool;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Executor based ThreadPool used in quartz scheduler.
 */
public final class ExecutorThreadPool implements ThreadPool {

  private static final int MAX_THREAD_POOL_SIZE = 500;
  private final ThreadPoolExecutor executor;

  public ExecutorThreadPool() {
    executor = createThreadPoolExecutor();
  }

  @Override
  public boolean runInThread(Runnable runnable) {
    executor.execute(runnable);
    return true;
  }

  @Override
  public int blockForAvailableThreads() {
    //Always accept new work. Additional runnables will be in the executor queue.
    return MAX_THREAD_POOL_SIZE;
  }


  @Override
  public void initialize() throws SchedulerConfigException {
  }

  @Override
  public void shutdown(boolean waitForJobsToComplete) {
    executor.shutdown();
  }


  @Override
  public int getPoolSize() {
    return MAX_THREAD_POOL_SIZE;
  }


  @Override
  public void setInstanceId(String schedInstId) {
    //no-op
  }

  @Override
  public void setInstanceName(String schedName) {
    //noop
  }

  private ThreadPoolExecutor createThreadPoolExecutor() {
    ThreadFactory threadFactory = new ThreadFactory() {
      private final ThreadGroup threadGroup = new ThreadGroup("scheduler-thread");
      private final AtomicLong count = new AtomicLong(0);

      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(threadGroup, r, String.format("scheduler-executor-%d", count.getAndIncrement()));
        t.setDaemon(true);
        return t;
      }
    };

    return new ThreadPoolExecutor(0, MAX_THREAD_POOL_SIZE,
                       60L, TimeUnit.SECONDS,
                       new SynchronousQueue<Runnable>(),
                       threadFactory, new ThreadPoolExecutor.AbortPolicy());
  }
}
