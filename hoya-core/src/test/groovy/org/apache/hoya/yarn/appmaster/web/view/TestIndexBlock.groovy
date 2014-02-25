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

import java.net.URL;
import java.util.Map;

import com.google.inject.AbstractModule
import com.google.inject.Guice
import com.google.inject.Injector
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
import org.apache.hoya.yarn.appmaster.web.WebAppApi
import org.apache.hoya.yarn.appmaster.web.WebAppApiImpl
import org.apache.hoya.yarn.model.mock.MockContainer
import org.apache.hoya.yarn.model.mock.MockContainerId
import org.apache.hoya.yarn.model.mock.MockNodeId
import org.apache.hoya.yarn.model.mock.MockRecordFactory
import org.apache.hoya.yarn.model.mock.MockResource
import org.apache.hoya.yarn.service.EventCallback
import org.junit.Before
import org.junit.Test

@Slf4j
@CompileStatic
public class TestIndexBlock {

  private IndexBlock indexBlock;

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

    indexBlock = injector.getInstance(IndexBlock.class);

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
  public void testIndex() {
    StringWriter sw = new StringWriter(64);
    PrintWriter pw = new PrintWriter(sw);

    Hamlet hamlet = new Hamlet(pw, 0, false);
    
    int level = hamlet.nestLevel();
    indexBlock.doIndex(hamlet, "accumulo");
    
    assert level == hamlet.nestLevel();
  }
  
  // non-implemented stubs to use Guice for injection
  
  private static class StubHoyaClusterProtocol implements HoyaClusterProtocol {

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
      return 0;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
      return null;
    }

    @Override
    public StopClusterResponseProto stopCluster(StopClusterRequestProto request) throws IOException, YarnException {
      return null;
    }

    @Override
    public FlexClusterResponseProto flexCluster(FlexClusterRequestProto request) throws IOException, YarnException {
      return null;
    }

    @Override
    public GetJSONClusterStatusResponseProto getJSONClusterStatus(GetJSONClusterStatusRequestProto request) throws IOException, YarnException {
      return null;
    }

    @Override
    public ListNodeUUIDsByRoleResponseProto listNodeUUIDsByRole(ListNodeUUIDsByRoleRequestProto request) throws IOException, YarnException {
      return null;
    }

    @Override
    public GetNodeResponseProto getNode(GetNodeRequestProto request) throws IOException, YarnException {
      return null;
    }

    @Override
    public GetClusterNodesResponseProto getClusterNodes(GetClusterNodesRequestProto request) throws IOException, YarnException {
      return null;
    }

    @Override
    public EchoResponseProto echo(EchoRequestProto request) throws IOException, YarnException {
      return null;
    }

    @Override
    public KillContainerResponseProto killContainer(KillContainerRequestProto request) throws IOException, YarnException {
      return null;
    }

    @Override
    public AMSuicideResponseProto amSuicide(AMSuicideRequestProto request) throws IOException, YarnException {
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
      return null;
    }

    @Override
    public List<ProviderRole> getRoles() {
      return null;
    }

    @Override
    public Configuration getConf() {
      return null;
    }

    @Override
    public void validateClusterSpec(ClusterDescription clusterSpec) throws HoyaException {

    }

    @Override
    public void init(Configuration config) {

    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void registerServiceListener(ServiceStateChangeListener listener) {

    }

    @Override
    public void unregisterServiceListener(ServiceStateChangeListener listener) {

    }

    @Override
    public Configuration getConfig() {
      return null;
    }

    @Override
    public STATE getServiceState() {
      return null;
    }

    @Override
    public long getStartTime() {
      return 0;
    }

    @Override
    public boolean isInState(STATE state) {
      return false;
    }

    @Override
    public Throwable getFailureCause() {
      return null;
    }

    @Override
    public STATE getFailureState() {
      return null;
    }

    @Override
    public boolean waitForServiceToStop(long timeout) {
      return false;
    }

    @Override
    public List<LifecycleEvent> getLifecycleHistory() {
      return null;
    }

    @Override
    public Map<String,String> getBlockers() {
      return null;
    }

    @Override
    public int getExitCode() {
      return 0;
    }

    @Override
    public void buildContainerLaunchContext(ContainerLaunchContext ctx, HoyaFileSystem hoyaFileSystem, Path generatedConfPath, String role,
        ClusterDescription clusterSpec, Map<String,String> roleOptions) throws IOException, HoyaException {

    }

    @Override
    public int getDefaultMasterInfoPort() {
      return 0;
    }

    @Override
    public boolean exec(ClusterDescription cd, File confDir, Map<String,String> env, EventCallback execInProgress) throws IOException, HoyaException {
      return false;
    }

    @Override
    public boolean isSupportedRole(String role) {
      return false;
    }

    @Override
    public Configuration loadProviderConfigurationInformation(File confDir) throws BadCommandArgumentsException, IOException {
      return null;
    }

    @Override
    public void validateApplicationConfiguration(ClusterDescription clusterSpec, File confDir, boolean secure) throws IOException, HoyaException {

    }

    @Override
    public boolean initMonitoring() {
      return false;
    }

    @Override
    public List<Probe> createProbes(ClusterDescription clusterSpec, String url, Configuration config, int timeout) throws IOException {
      return null;
    }

    @Override
    public Map<String,String> buildProviderStatus() {
      return null;
    }

    @Override
    public void buildContainerLaunchContext(ContainerLaunchContext ctx,
        Container container, String role, HoyaFileSystem hoyaFileSystem,
        Path generatedConfPath, ClusterDescription clusterSpec,
        Map<String, String> roleOptions, Path containerTmpDirPath)
        throws IOException, HoyaException {
      
    }

    @Override
    public Map<String, URL> buildMonitorDetails(ClusterDescription clusterSpec) {
      return null;
    }
  }
}