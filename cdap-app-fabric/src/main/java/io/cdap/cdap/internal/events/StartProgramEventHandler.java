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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically checks event reader for StartProgramEvents. If any events available, publish them to
 * TMS.
 */
public class StartProgramEventHandler extends EventHandler {

  private static final Logger LOG = LoggerFactory.getLogger(StartProgramEventHandler.class);
  private EventReader reader;
  private final CConfiguration cConf;
  private final ProgramLifecycleService lifecycleService;
  private ScheduledExecutorService executor;

  /**
   * Create instance that handles StartProgramEvents.
   *
   * @param cConf CDAP configuration
   * @param lifecycleService to publish start programs to TMS
   */
  @Inject
  StartProgramEventHandler(CConfiguration cConf,
                           ProgramLifecycleService lifecycleService) {
    this.cConf = cConf;
    this.lifecycleService = lifecycleService;
  }

  @Override
  public void initialize(EventReader eventReader) {
    this.reader = eventReader;
    this.reader.initialize(new DefaultEventReaderContext(cConf, reader.getClass().getName()));
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(0,
            cConf.getInt(Constants.Event.START_PROGRAM_EVENT_READER_POLL_DELAY), TimeUnit.SECONDS);
  }

  @Override
  protected void runOneIteration() throws Exception {
    EventResult<StartProgramEvent> result = reader.pull(1);
    result.consumeEvents(this::startProgram);
  }

  /**
   * Attempt to publish program to TMS.
   *
   * @param event Event containing program info
   * @throws RuntimeException if starting program fails
   */
  private void startProgram(StartProgramEvent event) {
    try {
      StartProgramEventDetails eventDetails = event.getEventDetails();
      ProgramType programType = ProgramType.valueOfCategoryName(eventDetails.getProgramType());
      ProgramReference programReference = new ProgramReference(eventDetails.getNamespaceId(),
              eventDetails.getAppId(), programType,
              eventDetails.getProgramId());
      LOG.debug("Starting pipeline {}, with args: {}", eventDetails.getAppId(), eventDetails.getArgs());
      RunId runId = lifecycleService.run(programReference, eventDetails.getArgs(), true);
      LOG.info("Started pipeline, RunId: {}", runId);
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
    LOG.info("StartProgramEventHandler started.");
  }

  @Override
  protected void shutDown() throws Exception {
    if (executor != null) {
      executor.shutdownNow();
    }
    LOG.info("StartProgramEventHandler successfully shut down.");
  }
}
