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

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.feature.DefaultFeatureFlagsProvider;
import io.cdap.cdap.features.Feature;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EventHandlerManager is responsible for starting all the event handler threads.
 */
public class EventHandlerManager extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(EventHandlerManager.class);

  private final boolean enabled;
  private final Set<EventHandler> eventHandlers;

  @Inject
  EventHandlerManager(CConfiguration cConf, Set<EventHandler> eventHandlers) {
    this.enabled = Feature.EVENT_READER.isEnabled(new DefaultFeatureFlagsProvider(cConf));
    this.eventHandlers = eventHandlers;
  }

  @Override
  protected void startUp() throws Exception {
    if (!enabled) {
      return; // If not enabled, don't start
    }
    eventHandlers.forEach(eventHandler -> {
      // Loading the event writers from provider
      // Initialize the event publisher with all the event writers provided by provider
      if (eventHandler.initialize()) {
        eventHandler.startAndWait();
        LOG.info("Successfully initialized eventReaderHandler: {}",
                eventHandler.getClass().getSimpleName());
      } else {
        LOG.error("Failed to initialize eventReaderHandler: {}",
                eventHandler.getClass().getSimpleName());
      }
    });
  }

  @Override
  protected void shutDown() throws Exception {
    if (!enabled) {
      return; // If not enabled, don't shut down
    }
    eventHandlers.forEach(eventHandler -> {
      eventHandler.stopAndWait();
    });
  }
}
