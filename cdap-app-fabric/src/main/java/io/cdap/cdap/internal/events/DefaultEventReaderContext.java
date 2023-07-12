/*
 * Copyright © 2023 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.internal.events;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.spi.events.EventReaderContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides an initialized default context for EventReader implementing {@link EventReaderContext}.
 */
public class DefaultEventReaderContext implements EventReaderContext {

  private final Map<String, String> properties;

  /**
   * Construct the default Event reader context.
   *
   * @param prefix prefix for specific event reader (e.g start.simpleclassname)
   * @param cConf        An instance of an injected ${@link CConfiguration}.
   */
  DefaultEventReaderContext(String prefix, CConfiguration cConf) {
    String completePrefix = String.format("%s.%s", Constants.Event.EVENTS_READER_PREFIX, prefix);

    // Calculate the maximum TMS retry timeout
    int retryTimeout = cConf.getInt(Constants.AppFabric.PROGRAM_STATUS_RETRY_STRATEGY_PREFIX
            + Constants.Retry.MAX_TIME_SECS);
    // Set the acknowledgement deadline to be greater than the retry timeout by configurable amount
    int ackDeadline = retryTimeout + cConf.getInt(Constants.Event.EVENTS_READER_ACK_BUFFER);
    Map<String, String> mutableProperties = new HashMap<>(cConf.getPropsWithPrefix(completePrefix));
    mutableProperties.put("ackDeadline", String.valueOf(ackDeadline));
    this.properties = Collections.unmodifiableMap(mutableProperties);
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }
}
