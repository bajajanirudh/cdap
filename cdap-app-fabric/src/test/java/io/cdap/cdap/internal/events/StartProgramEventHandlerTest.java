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

import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.events.dummy.DummyEventReader;
import io.cdap.cdap.internal.events.dummy.DummyEventReaderExtensionProvider;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramReference;
import io.cdap.cdap.spi.events.EventReader;
import io.cdap.cdap.spi.events.StartProgramEvent;
import io.cdap.cdap.spi.events.StartProgramEventDetails;
import org.junit.*;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Tests for the {@link StartProgramEventHandler}.
 */
public class StartProgramEventHandlerTest extends AppFabricTestBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(StartProgramEventHandlerTest.class);
  @Mock
  private ProgramLifecycleService lifecycleService;
  @Mock
  CConfiguration cConf;
  @InjectMocks
  private StartProgramEventHandler eventHandler;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    cConf.setInt(Constants.AppFabric.PROGRAM_STATUS_RETRY_STRATEGY_PREFIX
            + Constants.Retry.MAX_TIME_SECS, 6000);
    cConf.setInt(Constants.Event.EVENTS_READER_ACK_BUFFER, 0);
  }


  @Test
  public void testInitialize() {

    EventReaderProvider provider = new DummyEventReaderExtensionProvider(new DummyEventReader());
    Map<String, EventReader> eventReaderMap = provider.loadEventReaders();
    try {
      eventHandler.initialize(eventReaderMap.values().iterator().next());
    } catch (Exception ex) {
      LOGGER.error("Error during Event Handler initialization.", ex);
      Assert.fail("Error while initializing Event Handler");
    }
  }

  @Test
  public void testMessageWorkflow() throws Exception {
    assert (lifecycleService != null);
    Mockito.doReturn(RunIds.generate()).when(lifecycleService).run((ProgramReference) any(), any(), anyBoolean());
    DummyEventReader<StartProgramEvent> eventReader = Mockito.mock(DummyEventReader.class);
    when(eventReader.getMessages()).thenReturn(mockedEvents());

    Mockito.doCallRealMethod().when(eventReader).pull(1);
    Mockito.doCallRealMethod().when(eventReader).initialize(any());
    EventReaderProvider provider = new DummyEventReaderExtensionProvider(eventReader);
    Map<String, EventReader> eventReaderMap = provider.loadEventReaders();

    try {
      eventHandler.initialize(eventReaderMap.values().iterator().next());
    } catch (Exception ex) {
      LOGGER.error("Error during Event Handler initialization.", ex);
      Assert.fail("Error while initializing Event Handler");
    }
    try {
      eventHandler.runOneIteration();
    } catch (Exception e) {
      LOGGER.error("Error during message process.", e);
      Assert.fail("Error during message process");
    }
    // FIXME: Error no invocation
    verify(lifecycleService).run((ProgramReference) any(), any(), anyBoolean());
  }

  private Collection<StartProgramEvent> mockedEvents() {
    ArrayList<StartProgramEvent> eventList = new ArrayList<>();
    eventList.add(new StartProgramEvent(1, "1", new StartProgramEventDetails("app1",
            "namespace1", "id1", "workflows", null)));
    return eventList;
  }
}
