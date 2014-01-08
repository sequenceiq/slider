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

package org.apache.hadoop.hoya.yarn.model.appstate

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.hoya.api.ClusterDescription
import org.apache.hadoop.hoya.api.RoleKeys
import org.apache.hadoop.hoya.yarn.appmaster.state.AbstractRMOperation
import org.apache.hadoop.hoya.yarn.appmaster.state.ContainerRequestOperation
import org.apache.hadoop.hoya.yarn.model.mock.BaseMockAppStateTest
import org.apache.hadoop.hoya.yarn.model.mock.MockRoles
import org.apache.hadoop.yarn.api.records.Resource
import org.junit.Test
/**
 * Test the container resource allocation logic
 */
@CompileStatic
@Slf4j
class TestContainerResourceAllocations extends BaseMockAppStateTest {

  @Override
  String getTestName() {
    "TestContainerResourceAllocations"
  }

  @Test
  public void testNormalAllocations() throws Throwable {
    ClusterDescription clusterSpec = factory.newClusterSpec(1, 0, 0)
    clusterSpec.setRoleOpt(MockRoles.ROLE0, RoleKeys.YARN_MEMORY, 512)
    clusterSpec.setRoleOpt(MockRoles.ROLE0, RoleKeys.YARN_CORES, 2)
    appState.updateClusterSpec(clusterSpec)
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    assert ops.size() == 1
    ContainerRequestOperation operation = (ContainerRequestOperation) ops[0]
    Resource requirements = operation.request.capability
    assert requirements.memory == 512
    assert requirements.virtualCores == 2
  }

  @Test
  public void testMaxMemAllocations() throws Throwable {
    ClusterDescription clusterSpec = factory.newClusterSpec(1, 0, 0)
    clusterSpec.setRoleOpt(MockRoles.ROLE0, RoleKeys.YARN_MEMORY,
                           RoleKeys.YARN_RESOURCE_MAX)
    clusterSpec.setRoleOpt(MockRoles.ROLE0, RoleKeys.YARN_CORES, 2)
    appState.updateClusterSpec(clusterSpec)
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    assert ops.size() == 1
    ContainerRequestOperation operation = (ContainerRequestOperation) ops[0]
    Resource requirements = operation.request.capability
    assert requirements.memory == RM_MAX_RAM
    assert requirements.virtualCores == 2
  }
  
  @Test
  public void testMaxCoreAllocations() throws Throwable {
    ClusterDescription clusterSpec = factory.newClusterSpec(1, 0, 0)
    clusterSpec.setRoleOpt(MockRoles.ROLE0, RoleKeys.YARN_MEMORY,
                           512)
    clusterSpec.setRoleOpt(MockRoles.ROLE0, RoleKeys.YARN_CORES,
                           RoleKeys.YARN_RESOURCE_MAX)
    appState.updateClusterSpec(clusterSpec)
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    assert ops.size() == 1
    ContainerRequestOperation operation = (ContainerRequestOperation) ops[0]
    Resource requirements = operation.request.capability
    assert requirements.memory == 512
    assert requirements.virtualCores == RM_MAX_CORES
  }
  
  @Test
  public void testMaxDefaultAllocations() throws Throwable {
    ClusterDescription clusterSpec = factory.newClusterSpec(1, 0, 0)
    appState.updateClusterSpec(clusterSpec)
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    assert ops.size() == 1
    ContainerRequestOperation operation = (ContainerRequestOperation) ops[0]
    Resource requirements = operation.request.capability
    assert requirements.memory == RoleKeys.DEF_YARN_MEMORY
    assert requirements.virtualCores == RoleKeys.DEF_YARN_CORES
  }

  @Test
  public void testLimitsInClusterStatus() throws Throwable {
    appState.refreshClusterStatus(null)
    ClusterDescription cd = appState.clusterDescription
    assert cd.info[RoleKeys.YARN_MEMORY] == Integer.toString(RM_MAX_RAM)
    assert cd.info[RoleKeys.YARN_CORES] == Integer.toString(RM_MAX_CORES)
  }
}
