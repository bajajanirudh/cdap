/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed.runtimejob;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.app.runtime.ProgramRunnerFactory;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.twill.NoopTwillRunnerService;
import io.cdap.cdap.internal.app.deploy.ConfiguratorFactory;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.provisioner.Cluster;
import io.cdap.cdap.runtime.spi.provisioner.ClusterStatus;
import io.cdap.cdap.runtime.spi.provisioner.Node;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobEnvironment;
import java.util.Collections;
import java.util.Map;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit test for {@link DefaultRuntimeJob}.
 */
public class DefaultRuntimeJobTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testInjector() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().toString());

    LocationFactory locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFile());

    DefaultRuntimeJob defaultRuntimeJob = new DefaultRuntimeJob();
    Arguments systemArgs = new BasicArguments(Collections.singletonMap(SystemArguments.PROFILE_NAME, "test"));
    Node node = new Node("test", Node.Type.MASTER, "127.0.0.1", System.currentTimeMillis(), Collections.emptyMap());
    Cluster cluster = new Cluster("test", ClusterStatus.RUNNING, Collections.singleton(node), Collections.emptyMap());
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app").workflow("workflow").run(RunIds.generate());
    Arguments userArgs = new BasicArguments(Collections.singletonMap("program.runtime.mode", "LOCAL"));
    //Arguments userArgs = new BasicArguments();
    SimpleProgramOptions programOpts = new SimpleProgramOptions(programRunId.getParent(), systemArgs,
                                                                userArgs);

    RuntimeJobEnvironment runtimeJobEnvironment = new RuntimeJobEnvironment() {
      @Override
      public LocationFactory getLocationFactory() {
        return locationFactory;
      }

      @Override
      public TwillRunner getTwillRunner() {
        return new NoopTwillRunnerService();
      }

      @Override
      public Map<String, String> getProperties() {
        return Collections.emptyMap();
      }
    };
    Injector injector = Guice.createInjector(
      defaultRuntimeJob.createModules(runtimeJobEnvironment, cConf, programRunId, programOpts));

    injector.getInstance(LogAppenderInitializer.class);
    defaultRuntimeJob.createCoreServices(injector, systemArgs, cluster);
    injector.getInstance(ConfiguratorFactory.class);
    ProgramRunnerFactory programRunnerFactory = injector.getInstance(ProgramRunnerFactory.class);
    ProgramRunner programRunner = programRunnerFactory.create(ProgramType.SPARK);
    int x = 0;
  }
}
