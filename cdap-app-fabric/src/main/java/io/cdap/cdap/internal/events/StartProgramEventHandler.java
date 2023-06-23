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

import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.feature.DefaultFeatureFlagsProvider;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramReference;
import io.cdap.cdap.spi.events.EventReader;
import io.cdap.cdap.spi.events.EventResult;
import io.cdap.cdap.spi.events.StartProgramEvent;
import io.cdap.cdap.spi.events.StartProgramEventDetails;
import org.apache.twill.api.RunId;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StartProgramEventHandler extends AbstractScheduledService {

  private static final Logger logger = Logger.getLogger(EventReader.class.getName());
  private final boolean enabled;
  private final EventReaderExtensionProvider readerExtensionProvider;
  private final EventReader reader;
  private final ProgramLifecycleService lifecycleService;

  @Inject
  StartProgramEventHandler(CConfiguration cConf,
                           EventReaderExtensionProvider readerExtensionProvider,
                           ProgramLifecycleService lifecycleService) {
    this.enabled = Feature.EVENT_READ.isEnabled(new DefaultFeatureFlagsProvider(cConf));
    this.readerExtensionProvider = readerExtensionProvider;
    // TODO: How to get one
    reader = readerExtensionProvider.getAll().values().iterator().next();

    reader.initialize(new DefaultEventReaderContext(cConf, reader.getClass().getName()));
    this.lifecycleService = lifecycleService;
  }


  @Override
  protected void runOneIteration() throws Exception {
    EventResult<StartProgramEvent> result = reader.pull(1);
    result.consumeEvents(this::startProgram);
  }

  private void startProgram(StartProgramEvent event) {
    try {

      StartProgramEventDetails eventDetails = event.getEventDetails();
      ProgramType programType = ProgramType.valueOfCategoryName(eventDetails.getProgramType());
      ProgramReference programReference = new ProgramReference(eventDetails.getNamespaceId(),
              eventDetails.getAppId(), programType,
              eventDetails.getProgramId());
      logger.log(Level.FINE, "Starting pipeline " + eventDetails.getAppId() + ", with args: "
              + eventDetails.getArgs());
      RunId runId = lifecycleService.run(programReference, eventDetails.getArgs(), true);
      logger.log(Level.FINE, "Started pipeline, RunId: " + runId);
    } catch (Exception e) {
      // TODO: VERY SUSPECT
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(1, 1, TimeUnit.SECONDS);
  }
}
