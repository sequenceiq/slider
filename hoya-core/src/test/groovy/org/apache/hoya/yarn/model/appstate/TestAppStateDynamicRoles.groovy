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

package org.apache.hoya.yarn.model.appstate

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records.ContainerId
import org.apache.hoya.api.ClusterDescription
import org.apache.hoya.api.RoleKeys
import org.apache.hoya.yarn.appmaster.state.AbstractRMOperation
import org.apache.hoya.yarn.appmaster.state.AppState
import org.apache.hoya.yarn.appmaster.state.RoleInstance
import org.apache.hoya.yarn.model.mock.BaseMockAppStateTest
import org.apache.hoya.yarn.model.mock.MockRecordFactory
import org.apache.hoya.yarn.model.mock.MockRoles
import org.apache.hoya.yarn.model.mock.MockYarnEngine
import org.junit.Test

/**
 * Test that if you have >1 role, the right roles are chosen for release.
 */
@CompileStatic
@Slf4j
class TestAppStateDynamicRoles extends BaseMockAppStateTest
    implements MockRoles {

  @Override
  String getTestName() {
    return "TestAppStateDynamicRoles"
  }

  /**
   * Small cluster with multiple containers per node,
   * to guarantee many container allocations on each node
   * @return
   */
  @Override
  MockYarnEngine createYarnEngine() {
    return new MockYarnEngine(4, 4)
  }

  @Override
  void initApp() {
    super.initApp()
    appState = new AppState(new MockRecordFactory())
    appState.setContainerLimits(RM_MAX_RAM, RM_MAX_CORES)

    def cd = factory.newClusterSpec(0, 0, 0)

    def opts = [
        (RoleKeys.ROLE_INSTANCES): "1",
        (RoleKeys.ROLE_PRIORITY): "6",
    ]
    
    cd.roles["dynamic"]= opts
    
    appState.buildInstance(cd,
        new Configuration(false),
        factory.ROLES,
        fs,
        historyPath,
        null)
  }

  @Test
  public void testAllocateReleaseRealloc() throws Throwable {

    List<RoleInstance> instances = createAndStartNodes()
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    appState.getRoleHistory().dump();
    
  }
  
}
