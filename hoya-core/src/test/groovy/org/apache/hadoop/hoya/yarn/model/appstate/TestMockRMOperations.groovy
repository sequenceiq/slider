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
import org.apache.hadoop.hoya.exceptions.HoyaRuntimeException
import org.apache.hadoop.hoya.yarn.appmaster.state.AbstractRMOperation
import org.apache.hadoop.hoya.yarn.appmaster.state.ContainerAssignment
import org.apache.hadoop.hoya.yarn.appmaster.state.ContainerReleaseOperation
import org.apache.hadoop.hoya.yarn.appmaster.state.ContainerRequestOperation
import org.apache.hadoop.hoya.yarn.appmaster.state.RMOperationHandler
import org.apache.hadoop.hoya.yarn.appmaster.state.RoleInstance
import org.apache.hadoop.hoya.yarn.model.mock.BaseMockAppStateTest
import org.apache.hadoop.hoya.yarn.model.mock.MockRMOperationHandler
import org.apache.hadoop.hoya.yarn.model.mock.MockRoles
import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.junit.Test

@CompileStatic
@Slf4j
class TestMockRMOperations extends BaseMockAppStateTest implements MockRoles {

  @Test
  public void testMockAddOp() throws Throwable {
    role1Status.desired = 1
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    assert ops.size() == 1
    ContainerRequestOperation operation = (ContainerRequestOperation) ops[0]
    assert operation.request.priority.priority == 1
    RMOperationHandler handler = new MockRMOperationHandler()
    handler.execute(ops)

    //tell the container its been allocated
    AbstractRMOperation op = handler.operations[0]
    assert op instanceof ContainerRequestOperation
  }


  @Test
  public void testAllocateReleaseOp() throws Throwable {
    role1Status.desired = 1

    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    ContainerRequestOperation operation = (ContainerRequestOperation) ops[0]
    AMRMClient.ContainerRequest request = operation.request
    Container cont = engine.allocateContainer(request)
    List<ContainerAssignment> assignments = [];
    List<AbstractRMOperation> operations = []
    appState.onContainersAllocated([cont], assignments, operations)
    assert operations.size() == 0
    assert assignments.size() == 1
    ContainerAssignment assigned = assignments[0]
    Container target = assigned.container
    assert target.id == cont.id
    int roleId = assigned.role.priority
    assert roleId == request.priority.priority
    assert assigned.role.name == ROLE1
    RoleInstance ri = buildInstance(assigned)
    //tell the app it arrived
    appState.containerStartSubmitted(target, ri);
    appState.onNodeManagerContainerStartedFaulting(target.id)
    assert role1Status.started == 1

    //now release it by changing the role status
    role1Status.desired = 0
    ops = appState.reviewRequestAndReleaseNodes()
    assert ops.size() == 1

    assert ops[0] instanceof ContainerReleaseOperation
    ContainerReleaseOperation release = (ContainerReleaseOperation) ops[0]
    assert release.containerId == cont.id
  }

  @Test
  public void testComplexAllocation() throws Throwable {
    role1Status.desired = 1
    role2Status.desired = 3

    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    List<Container> allocations = engine.process(ops)
    List<ContainerAssignment> assignments = [];
    List<AbstractRMOperation> releases = []
    appState.onContainersAllocated(allocations, assignments, releases)
    assert releases.size() == 0
    assert assignments.size() == 4
    assignments.each { ContainerAssignment assigned ->
      Container target = assigned.container
      RoleInstance ri = buildInstance(assigned)
      appState.containerStartSubmitted(target, ri);
    }
    //insert some async operation here
    assignments.each { ContainerAssignment assigned ->
      Container target = assigned.container
      appState.onNodeManagerContainerStartedFaulting(target.id)
    }
    assert engine.containerCount() == 4;
    role2Status.desired = 0
    ops = appState.reviewRequestAndReleaseNodes()
    assert ops.size() == 3
    allocations = engine.process(ops)
    assert engine.containerCount() == 1;

    appState.onContainersAllocated(allocations, assignments, releases)
    assert assignments.empty
    assert releases.empty
  }

  @Test
  public void testDoubleNodeManagerStartEvent() throws Throwable {
    role1Status.desired = 1

    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    List<Container> allocations = engine.process(ops)
    List<ContainerAssignment> assignments = [];
    List<AbstractRMOperation> releases = []
    appState.onContainersAllocated(allocations, assignments, releases)
    assert assignments.size() == 1
    ContainerAssignment assigned = assignments[0]
    Container target = assigned.container
    RoleInstance ri = buildInstance(assigned)
    appState.containerStartSubmitted(target, ri);
    RoleInstance ri2 = appState.onNodeManagerContainerStartedFaulting(target.id)
    assert ri2 == ri
    //try a second time, expect an error
    try {
      appState.onNodeManagerContainerStartedFaulting(target.id)
      fail("Expected an exception")
    } catch (HoyaRuntimeException expected) {
      // expected
    }
    //and non-faulter should not downgrade to a null
    log.warn("Ignore any exception/stack trace that appears below")
    RoleInstance ri3 = appState.onNodeManagerContainerStarted(target.id)

    assert ri3 == null

  }



}
