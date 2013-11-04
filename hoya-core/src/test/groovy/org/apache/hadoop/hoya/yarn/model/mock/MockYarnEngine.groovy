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

package org.apache.hadoop.hoya.yarn.model.mock

import org.apache.hadoop.hoya.yarn.appmaster.state.AbstractRMOperation
import org.apache.hadoop.hoya.yarn.appmaster.state.ContainerReleaseOperation
import org.apache.hadoop.hoya.yarn.appmaster.state.ContainerRequestOperation
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.api.records.ContainerId
import org.apache.hadoop.yarn.api.records.NodeId
import org.apache.hadoop.yarn.api.records.Priority
import org.apache.hadoop.yarn.api.records.Resource
import org.apache.hadoop.yarn.client.api.AMRMClient

class MockYarnEngine {

  int containerId = 0;
  int nodes = 1;
  MockYarnCluster cluster;
  Allocator allocator;

  Map<Integer, MockContainer> activeContainers = [:]

  ApplicationId appId = new MockApplicationId(
      id: 0,
      clusterTimestamp: 0,
      )
  
  ApplicationAttemptId attemptId = new MockApplicationAttemptId(
      applicationId: appId,
      attemptId: 1,
      )
  
  NodeId nodeId = new MockNodeId("host-$nodes", 80)

  int containerCount() {
    return activeContainers.size();
  }

  void reset() {
    activeContainers = [:]
    containerId = 0
  }

  /**
   * Allocate a container from a request. The containerID will be
   * unique, nodeId and other fields chosen internally with
   * no such guarantees; resource and priority copied over
   * @param request request
   * @return container
   */
  Container allocateContainer(AMRMClient.ContainerRequest request) {
    Resource resource = request.getCapability();
    Priority pri = request.getPriority();
    int containerNo = containerId++
    ContainerId cid = new MockContainerId(
        id: containerNo,
        applicationAttemptId: attemptId
    )

    MockContainer container = new MockContainer(
        id: cid,
        priority: request.priority,
        resource: resource,
        nodeId: nodeId,

        nodeHttpAddress: "http://${nodeId.host}/"
    )
    activeContainers[containerNo] = container
    return container
  }

  boolean releaseContainer(ContainerId containerId) {
    MockContainer container = activeContainers.remove(containerId.id)
    return container != null
  }

  /**
   * Process a list of operations
   * @param ops
   * @return
   */
  List<Container> process(List<AbstractRMOperation> ops) {
    List<Container> allocation = [];
    ops.each { AbstractRMOperation op ->
      if (op instanceof ContainerReleaseOperation) {
        ContainerReleaseOperation cro = (ContainerReleaseOperation) op
        releaseContainer(cro.containerId);
      } else {
        ContainerRequestOperation req = (ContainerRequestOperation) op
        allocation.add(allocateContainer(req.request))
      }
    }
    return allocation
  }

}