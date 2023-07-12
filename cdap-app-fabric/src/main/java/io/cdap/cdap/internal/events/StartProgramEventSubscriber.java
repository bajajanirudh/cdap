/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.events;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramReference;
import io.cdap.cdap.spi.events.EventReader;
import io.cdap.cdap.spi.events.EventResult;
import io.cdap.cdap.spi.events.StartProgramEvent;
import io.cdap.cdap.spi.events.StartProgramEventDetails;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically checks event reader for StartProgramEvents. If any events available, publish them to
 * TMS.
 */
public class StartProgramEventSubscriber extends EventSubscriber {

  private static final Logger LOG = LoggerFactory.getLogger(StartProgramEventSubscriber.class);

  private final CConfiguration cConf;
  private final EventReaderProvider<StartProgramEvent> extensionProvider;
  private final ProgramLifecycleService lifecycleService;
  private ScheduledExecutorService executor;
  private Collection<EventReader<StartProgramEvent>> readers;
  private ExecutorService threadPoolExecutor;

  /**
   * Create instance that handles StartProgramEvents.
   *
   * @param cConf CDAP configuration
   * @param extensionProvider eventReaderProvider for StartProgramEvent Readers
   * @param lifecycleService to publish start programs to TMS
   */
  @Inject
  StartProgramEventSubscriber(CConfiguration cConf, EventReaderProvider<StartProgramEvent> extensionProvider,
                              ProgramLifecycleService lifecycleService) {
    this.cConf = cConf;
    this.extensionProvider = extensionProvider;
    this.lifecycleService = lifecycleService;
  }

  @Override
  public boolean initialize() {
    readers = extensionProvider.loadEventReaders().values();
    if (readers.isEmpty()) {
      return false;
    }
    threadPoolExecutor = new ThreadPoolExecutor(readers.size(), readers.size(), 60,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>());
    for (EventReader reader : readers) {
      reader.initialize(
              new DefaultEventReaderContext(Constants.Event.START_EVENT_PREFIX + "."
                      + reader.getClass().getSimpleName(), cConf));
    }
    return true;
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(0,
            cConf.getInt(Constants.Event.START_PROGRAM_EVENT_READER_POLL_DELAY), TimeUnit.SECONDS);
  }

  @Override
  protected void runOneIteration() throws Exception {
    if (threadPoolExecutor != null) {
      for (EventReader<StartProgramEvent> reader : readers) {
        threadPoolExecutor.execute(() -> {
          EventResult<StartProgramEvent> result = reader.pull(1);
          result.consumeEvents(this::startProgram);
        });
      }
    }
  }

  /**
   * Attempt to publish program to TMS.
   *
   * @param event Event containing program info
   * @throws RuntimeException if starting program fails
   */
  private void startProgram(StartProgramEvent event) {
      StartProgramEventDetails eventDetails = event.getEventDetails();
    try {
      ProgramType programType = ProgramType.valueOfCategoryName(eventDetails.getProgramType());
      ProgramReference programReference = new ProgramReference(eventDetails.getNamespaceId(),
              eventDetails.getAppId(), programType,
              eventDetails.getProgramId());
      LOG.debug("Starting pipeline {}, with args: {}, programReference: {}",
              eventDetails.getAppId(), eventDetails.getArgs(), programReference);
      RunId runId = lifecycleService.run(programReference, eventDetails.getArgs(), true);
      LOG.info("Started pipeline, RunId: {}, ProgramReference: {}", runId, programReference);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected final ScheduledExecutorService executor() {
    executor =
            Executors.newSingleThreadScheduledExecutor(
                    Threads.createDaemonThreadFactory("start-program-event-handler-scheduler"));
    return executor;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("StartProgramEventSubscriber started.");
  }

  @Override
  protected void shutDown() throws Exception {
    if (executor != null) {
      executor.shutdownNow();
    }
    if (threadPoolExecutor != null) {
      threadPoolExecutor.shutdownNow();
    }
    LOG.info("StartProgramEventSubscriber successfully shut down.");
  }

}
