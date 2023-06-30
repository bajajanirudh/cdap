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

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.events.dummy.DummyEventReader;
import io.cdap.cdap.spi.events.EventReader;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

/**
 * Tests for {@link EventReaderExtensionProvider}
 */
public class EventReaderExtensionProviderTest {

  @Test
  public void testEnabledEventReaderFilter() {
    EventReader mockReader = new DummyEventReader();
    String mockReaderName = mockReader.getClass().getName();
    CConfiguration cConf = CConfiguration.create();

    EventReaderExtensionProvider readerExtensionProvider1 = new EventReaderExtensionProvider(cConf);
    Set<String> test1 = readerExtensionProvider1.getSupportedTypesForProvider(mockReader);
    Assert.assertTrue(test1.isEmpty());

    //Test with reader ID enabled
    cConf.setStrings(Constants.Event.EVENTS_READER_EXTENSIONS_ENABLED_LIST, mockReaderName);
    EventReaderExtensionProvider readerExtensionProvider2 = new EventReaderExtensionProvider(cConf);
    Set<String> test2 = readerExtensionProvider2.getSupportedTypesForProvider(mockReader);
    Assert.assertTrue(test2.contains(mockReaderName));
  }
}
