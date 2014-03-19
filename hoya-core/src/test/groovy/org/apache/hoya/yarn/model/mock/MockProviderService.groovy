/*
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

package org.apache.hoya.yarn.model.mock

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.service.LifecycleEvent
import org.apache.hadoop.service.ServiceStateChangeListener
import org.apache.hadoop.service.Service.STATE
import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext
import org.apache.hoya.api.ClusterDescription
import org.apache.hoya.core.conf.AggregateConf
import org.apache.hoya.core.conf.MapOperations
import org.apache.hoya.exceptions.BadCommandArgumentsException
import org.apache.hoya.exceptions.HoyaException
import org.apache.hoya.providers.ProviderRole
import org.apache.hoya.providers.ProviderService
import org.apache.hoya.tools.HoyaFileSystem
import org.apache.hoya.yarn.service.EventCallback

class MockProviderService implements ProviderService {

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
  public boolean exec(
      AggregateConf instanceDefinition,
      File confDir,
      Map<String, String> env,
      EventCallback execInProgress) throws IOException, HoyaException {
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
  public Map<String,String> buildProviderStatus() {
    return null;
  }

  @Override
  void buildContainerLaunchContext(
      ContainerLaunchContext ctx,
      Container container,
      String role,
      HoyaFileSystem hoyaFileSystem,
      Path generatedConfPath,
      MapOperations roleOptions,
      Path containerTmpDirPath,
      AggregateConf instanceDefinition) throws IOException, HoyaException {

  }

  @Override
  public Map<String, URL> buildMonitorDetails(ClusterDescription clusterSpec) {
    return null;
  }
}
