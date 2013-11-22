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
import org.apache.hadoop.hoya.yarn.appmaster.state.AbstractRMOperation
import org.apache.hadoop.hoya.yarn.appmaster.state.ContainerAssignment
import org.apache.hadoop.hoya.yarn.appmaster.state.RoleInstance
import org.apache.hadoop.hoya.yarn.appmaster.state.RoleStatus
import org.apache.hadoop.hoya.yarn.model.mock.BaseMockAppStateTest
import org.apache.hadoop.hoya.yarn.model.mock.MockRoles
import org.apache.hadoop.hoya.yarn.model.mock.MockYarnEngine
import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.api.records.ContainerId
import org.junit.Test

import static org.apache.hadoop.hoya.yarn.appmaster.state.ContainerPriority.extractRole
/**
 * Test that if you have >1 role, the right roles are chosen for release.
 */
@CompileStatic
@Slf4j
class TestAppStateContainerFailure extends BaseMockAppStateTest
    implements MockRoles {

  @Override
  String getTestName() {
    return "TestAppStateContainerFailure"
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

  @Test
  public void testShortLivedFail() throws Throwable {

    role0Status.desired = 1
    List<RoleInstance> instances = createAndStartNodes()
    assert instances.size() == 1

    RoleInstance instance = instances[0]
    long created = instance.createTime
    long started = instance.startTime
    assert created > 0
    assert started >= created
    List<ContainerId> ids = extractContainerIds(instances, 0)

    ContainerId cid = ids[0]
    assert appState.isShortLived(instance)
    assert appState.onCompletedNode(containerStatus(cid, 1))
    RoleStatus status = role0Status
    assert status.failed == 1
    assert status.startFailed == 1

    //view the world
    appState.getRoleHistory().dump();
    
  }

  @Test
  public void testLongLivedFail() throws Throwable {

    role0Status.desired = 1
    List<RoleInstance> instances = createAndStartNodes()
    assert instances.size() == 1

    RoleInstance instance = instances[0]
    instance.startTime = System.currentTimeMillis() - 60 * 60 * 1000;
    assert !appState.isShortLived(instance)
    List<ContainerId> ids = extractContainerIds(instances, 0)

    ContainerId cid = ids[0]
    assert appState.onCompletedNode(containerStatus(cid, 1))
    RoleStatus status = role0Status
    assert status.failed == 1
    assert status.startFailed == 0

    //view the world
    appState.getRoleHistory().dump();
    
  }

}
