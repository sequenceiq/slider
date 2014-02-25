/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hoya.yarn.appmaster.web.view;

import java.io.IOException;
import java.util.Map;

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.ipc.ProtocolSignature
import org.apache.hadoop.service.LifecycleEvent
import org.apache.hadoop.service.ServiceStateChangeListener
import org.apache.hadoop.service.Service.STATE
import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext
import org.apache.hadoop.yarn.api.records.Priority
import org.apache.hadoop.yarn.exceptions.YarnException
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV
import org.apache.hoya.api.ClusterDescription
import org.apache.hoya.api.HoyaClusterProtocol
import org.apache.hoya.api.proto.Messages.AMSuicideRequestProto
import org.apache.hoya.api.proto.Messages.AMSuicideResponseProto
import org.apache.hoya.api.proto.Messages.EchoRequestProto
import org.apache.hoya.api.proto.Messages.EchoResponseProto
import org.apache.hoya.api.proto.Messages.FlexClusterRequestProto
import org.apache.hoya.api.proto.Messages.FlexClusterResponseProto
import org.apache.hoya.api.proto.Messages.GetClusterNodesRequestProto
import org.apache.hoya.api.proto.Messages.GetClusterNodesResponseProto
import org.apache.hoya.api.proto.Messages.GetJSONClusterStatusRequestProto
import org.apache.hoya.api.proto.Messages.GetJSONClusterStatusResponseProto
import org.apache.hoya.api.proto.Messages.GetNodeRequestProto
import org.apache.hoya.api.proto.Messages.GetNodeResponseProto
import org.apache.hoya.api.proto.Messages.KillContainerRequestProto
import org.apache.hoya.api.proto.Messages.KillContainerResponseProto
import org.apache.hoya.api.proto.Messages.ListNodeUUIDsByRoleRequestProto
import org.apache.hoya.api.proto.Messages.ListNodeUUIDsByRoleResponseProto
import org.apache.hoya.api.proto.Messages.StopClusterRequestProto
import org.apache.hoya.api.proto.Messages.StopClusterResponseProto
import org.apache.hoya.exceptions.BadCommandArgumentsException
import org.apache.hoya.exceptions.HoyaException
import org.apache.hoya.providers.ProviderRole
import org.apache.hoya.providers.ProviderService
import org.apache.hoya.servicemonitor.Probe
import org.apache.hoya.tools.HoyaFileSystem
import org.apache.hoya.yarn.appmaster.state.AbstractRecordFactory
import org.apache.hoya.yarn.appmaster.state.AppState
import org.apache.hoya.yarn.appmaster.state.RoleInstance
import org.apache.hoya.yarn.appmaster.web.WebAppApi
import org.apache.hoya.yarn.appmaster.web.WebAppApiImpl
import org.apache.hoya.yarn.appmaster.web.view.ContainerStatsBlock.TableContent
import org.apache.hoya.yarn.model.mock.MockContainer
import org.apache.hoya.yarn.model.mock.MockContainerId
import org.apache.hoya.yarn.model.mock.MockNodeId
import org.apache.hoya.yarn.model.mock.MockRecordFactory
import org.apache.hoya.yarn.model.mock.MockResource
import org.apache.hoya.yarn.service.EventCallback
import org.junit.Before
import org.junit.Test
import com.google.inject.AbstractModule
import com.google.inject.Guice
import com.google.inject.Injector

@Slf4j
@CompileStatic
public class ContainerStatsBlockTest {

  private ContainerStatsBlock statsBlock;

  private static class StubHoyaClusterProtocol implements HoyaClusterProtocol {

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
      return 0;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public StopClusterResponseProto stopCluster(StopClusterRequestProto request) throws IOException, YarnException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public FlexClusterResponseProto flexCluster(FlexClusterRequestProto request) throws IOException, YarnException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public GetJSONClusterStatusResponseProto getJSONClusterStatus(GetJSONClusterStatusRequestProto request) throws IOException, YarnException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public ListNodeUUIDsByRoleResponseProto listNodeUUIDsByRole(ListNodeUUIDsByRoleRequestProto request) throws IOException, YarnException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public GetNodeResponseProto getNode(GetNodeRequestProto request) throws IOException, YarnException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public GetClusterNodesResponseProto getClusterNodes(GetClusterNodesRequestProto request) throws IOException, YarnException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public EchoResponseProto echo(EchoRequestProto request) throws IOException, YarnException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public KillContainerResponseProto killContainer(KillContainerRequestProto request) throws IOException, YarnException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public AMSuicideResponseProto amSuicide(AMSuicideRequestProto request) throws IOException, YarnException {
      // TODO Auto-generated method stub
      return null;
    }
  }

  private static class StubAppState extends AppState {
    public StubAppState(AbstractRecordFactory recordFactory) {
      super(recordFactory);
    }
  }


  private static class StubProviderService implements ProviderService {

    @Override
    public String getName() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public List<ProviderRole> getRoles() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Configuration getConf() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void validateClusterSpec(ClusterDescription clusterSpec) throws HoyaException {
      // TODO Auto-generated method stub

    }

    @Override
    public void init(Configuration config) {
      // TODO Auto-generated method stub

    }

    @Override
    public void start() {
      // TODO Auto-generated method stub

    }

    @Override
    public void stop() {
      // TODO Auto-generated method stub

    }

    @Override
    public void close() throws IOException {
      // TODO Auto-generated method stub

    }

    @Override
    public void registerServiceListener(ServiceStateChangeListener listener) {
      // TODO Auto-generated method stub

    }

    @Override
    public void unregisterServiceListener(ServiceStateChangeListener listener) {
      // TODO Auto-generated method stub

    }

    @Override
    public Configuration getConfig() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public STATE getServiceState() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public long getStartTime() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public boolean isInState(STATE state) {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public Throwable getFailureCause() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public STATE getFailureState() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public boolean waitForServiceToStop(long timeout) {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public List<LifecycleEvent> getLifecycleHistory() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Map<String,String> getBlockers() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public int getExitCode() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public void buildContainerLaunchContext(ContainerLaunchContext ctx, HoyaFileSystem hoyaFileSystem, Path generatedConfPath, String role,
        ClusterDescription clusterSpec, Map<String,String> roleOptions) throws IOException, HoyaException {
      // TODO Auto-generated method stub

    }

    @Override
    public int getDefaultMasterInfoPort() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public boolean exec(ClusterDescription cd, File confDir, Map<String,String> env, EventCallback execInProgress) throws IOException, HoyaException {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean isSupportedRole(String role) {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public Configuration loadProviderConfigurationInformation(File confDir) throws BadCommandArgumentsException, IOException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void validateApplicationConfiguration(ClusterDescription clusterSpec, File confDir, boolean secure) throws IOException, HoyaException {
      // TODO Auto-generated method stub

    }

    @Override
    public boolean initMonitoring() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public List<Probe> createProbes(ClusterDescription clusterSpec, String url, Configuration config, int timeout) throws IOException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Map<String,String> buildProviderStatus() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void buildContainerLaunchContext(ContainerLaunchContext ctx,
        Container container, String role, HoyaFileSystem hoyaFileSystem,
        Path generatedConfPath, ClusterDescription clusterSpec,
        Map<String, String> roleOptions, Path containerTmpDirPath)
        throws IOException, HoyaException {
      // TODO Auto-generated method stub
      
    }
  }

  private Container cont1, cont2;

  @Before
  public void setup() {
    HoyaClusterProtocol clusterProto = new StubHoyaClusterProtocol();
    AppState appState = new StubAppState(new MockRecordFactory());
    ProviderService providerService = new StubProviderService();

    WebAppApiImpl inst = new WebAppApiImpl(clusterProto, appState, providerService);

    Injector injector = Guice.createInjector(new AbstractModule() {
          @Override
          protected void configure() {
            bind(WebAppApi.class).toInstance(inst);
          }
        });

    statsBlock = injector.getInstance(ContainerStatsBlock.class);

    cont1 = new MockContainer();
    cont1.id = new MockContainerId();
    ((MockContainerId) cont1.id).setId(0);
    cont1.nodeId = new MockNodeId();
    cont1.priority = Priority.newInstance(1);
    cont1.resource = new MockResource();

    cont2 = new MockContainer();
    cont2.id = new MockContainerId();
    ((MockContainerId) cont2.id).setId(1);
    cont2.nodeId = new MockNodeId();
    cont2.priority = Priority.newInstance(1);
    cont2.resource = new MockResource();
  }

  @Test
  public void testGetContainerInstances() {
    List<RoleInstance> roles = [
      new RoleInstance(cont1),
      new RoleInstance(cont2),
    ];
    Map<String, RoleInstance> map = statsBlock.getContainerInstances(roles);

    assert 2 == map.size();

    assert map.containsKey("mockcontainer_0");
    assert map.get("mockcontainer_0").equals(roles[0]);

    assert map.containsKey("mockcontainer_1");
    assert map.get("mockcontainer_1").equals(roles[1]);
  }

  @Test
  public void testGenerateRoleDetails() {
    StringWriter sw = new StringWriter(64);
    PrintWriter pw = new PrintWriter(sw);

    Hamlet hamlet = new Hamlet(pw, 0, false);
    DIV<Hamlet> div = hamlet.div();
    
    String detailsName = "testing";
    String selector = "selector";
    Map<TableContent,String> data = new HashMap<TableContent,String>();
    data.put(new ContainerStatsBlock.TableContent("Foo"), "bar");
    
    int levelPrior = hamlet.nestLevel();
    statsBlock.generateRoleDetails(div, selector, detailsName, data.entrySet());
    
    // Ensures that nesting levels are correct
    assert levelPrior == hamlet.nestLevel();
    
    // Close that div
    div._();
    
    // and pop a new one
    div = hamlet.div();
    
    // A null 2nd entry in the tuple implies only one column
    data.clear();
    data.put(new ContainerStatsBlock.TableContent("Bar"), null);
    
    levelPrior = hamlet.nestLevel();
    statsBlock.generateRoleDetails(div, selector, detailsName, data.entrySet());
    
    // Ensures that nesting levels are correct
    assert levelPrior == hamlet.nestLevel();
  }
}