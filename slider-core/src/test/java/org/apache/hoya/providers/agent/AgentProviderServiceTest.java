/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hoya.providers.agent;

import junit.framework.TestCase;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hoya.api.ClusterDescription;
import org.apache.hoya.api.OptionKeys;
import org.apache.hoya.api.StatusKeys;
import org.apache.hoya.core.conf.AggregateConf;
import org.apache.hoya.core.conf.ConfTree;
import org.apache.hoya.core.conf.ConfTreeOperations;
import org.apache.hoya.core.conf.MapOperations;
import org.apache.hoya.exceptions.HoyaException;
import org.apache.hoya.tools.HoyaFileSystem;
import org.apache.hoya.yarn.appmaster.state.StateAccessForProviders;
import org.apache.hoya.yarn.appmaster.web.rest.agent.HeartBeat;
import org.apache.hoya.yarn.appmaster.web.rest.agent.HeartBeatResponse;
import org.apache.hoya.yarn.appmaster.web.rest.agent.Register;
import org.apache.hoya.yarn.appmaster.web.rest.agent.RegistrationResponse;
import org.apache.hoya.yarn.appmaster.web.rest.agent.RegistrationStatus;
import org.apache.hoya.yarn.model.mock.MockContainerId;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;


public class AgentProviderServiceTest {
  protected static final Logger log =
      LoggerFactory.getLogger(AgentProviderServiceTest.class);

  @Test
  public void testRegistration() throws IOException {

    ConfTree tree = new ConfTree();
    tree.global.put(OptionKeys.INTERNAL_APPLICATION_IMAGE_PATH, ".");

    AgentProviderService aps = new AgentProviderService();
    ContainerLaunchContext ctx = createNiceMock(ContainerLaunchContext.class);
    AggregateConf instanceDefinition = new AggregateConf();

    instanceDefinition.setInternal(tree);
    instanceDefinition.setAppConf(tree);
    instanceDefinition.getAppConfOperations().getGlobalOptions().put(AgentKeys.APP_DEF, ".");
    instanceDefinition.getAppConfOperations().getGlobalOptions().put(AgentKeys.AGENT_CONF, ".");
    instanceDefinition.getAppConfOperations().getGlobalOptions().put(AgentKeys.AGENT_VERSION, ".");

    Container container = createNiceMock(Container.class);
    String role = "HBASE_MASTER";
    HoyaFileSystem hoyaFileSystem = createNiceMock(HoyaFileSystem.class);
    Path generatedConfPath = new Path(".", "test");
    MapOperations resourceComponent = new MapOperations();
    MapOperations appComponent = new MapOperations();
    Path containerTmpDirPath = new Path(".", "test");
    expect(hoyaFileSystem.createAmResource(anyObject(Path.class), anyObject(LocalResourceType.class)))
        .andReturn(createNiceMock(LocalResource.class));
    expect(container.getId()).andReturn(new MockContainerId(1)).anyTimes();
    StateAccessForProviders access = createNiceMock(StateAccessForProviders.class);

    AgentProviderService mockAps = Mockito.spy(aps);
    doReturn(access).when(mockAps).getStateAccessor();

    try {
      doNothing().when(mockAps).addInstallCommand(
          eq("HBASE_MASTER"),
          any(HeartBeatResponse.class),
          eq("scripts/hbase_master.py"));
    } catch (HoyaException e) {
    }

    expect(access.isApplicationLive()).andReturn(true).anyTimes();
    ClusterDescription desc = new ClusterDescription();
    desc.setInfo(StatusKeys.INFO_AM_HOSTNAME, "host1");
    desc.setInfo(StatusKeys.INFO_AM_WEB_PORT, "8088");
    desc.setInfo(OptionKeys.APPLICATION_NAME, "HBASE");
    desc.getOrAddRole("HBASE_MASTER").put(AgentKeys.COMPONENT_SCRIPT, "scripts/hbase_master.py");
    expect(access.getClusterStatus()).andReturn(desc).anyTimes();

    AggregateConf aggConf = new AggregateConf();
    ConfTreeOperations treeOps = aggConf.getAppConfOperations();
    treeOps.getOrAddComponent("HBASE_MASTER").put(AgentKeys.WAIT_HEARTBEAT, "0");
    expect(access.getInstanceDefinitionSnapshot()).andReturn(aggConf);
    replay(access, ctx, container, hoyaFileSystem);

    try {
      mockAps.buildContainerLaunchContext(ctx,
                                          instanceDefinition,
                                          container,
                                          role,
                                          hoyaFileSystem,
                                          generatedConfPath,
                                          resourceComponent,
                                          appComponent,
                                          containerTmpDirPath);
    } catch (HoyaException he) {
      log.warn(he.getMessage());
    } catch (IOException ioe) {
      log.warn(ioe.getMessage());
    }

    Register reg = new Register();
    reg.setResponseId(0);
    reg.setHostname("mockcontainer_1___HBASE_MASTER");
    RegistrationResponse resp = mockAps.handleRegistration(reg);
    TestCase.assertEquals(0, resp.getResponseId());
    TestCase.assertEquals(RegistrationStatus.OK, resp.getResponseStatus());

    HeartBeat hb = new HeartBeat();
    hb.setResponseId(1);
    hb.setHostname("mockcontainer_1___HBASE_MASTER");
    HeartBeatResponse hbr = mockAps.handleHeartBeat(hb);
    TestCase.assertEquals(2, hbr.getResponseId());
  }
}
