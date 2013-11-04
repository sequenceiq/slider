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

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.hoya.yarn.appmaster.state.AbstractRMOperation
import org.apache.hadoop.hoya.yarn.appmaster.state.ContainerReleaseOperation
import org.apache.hadoop.hoya.yarn.appmaster.state.ContainerRequestOperation
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.api.records.ContainerId
import org.apache.hadoop.yarn.client.api.AMRMClient

@CompileStatic
@Slf4j
class MockYarnEngine {

  MockYarnCluster cluster;
  Allocator allocator;

  ApplicationId appId = new MockApplicationId(
      id: 0,
      clusterTimestamp: 0,
      )

  ApplicationAttemptId attemptId = new MockApplicationAttemptId(
      applicationId: appId,
      attemptId: 1,
      )

  int containerCount() {
    return cluster.containersInUse();
  }

  MockYarnEngine(int clusterSize, int containersPerNode) {
    cluster = new MockYarnCluster(clusterSize, containersPerNode)
    allocator = new Allocator(cluster)
  }

/**
 * Allocate a container from a request. The containerID will be
 * unique, nodeId and other fields chosen internally with
 * no such guarantees; resource and priority copied over
 * @param request request
 * @return container
 */
  Container allocateContainer(AMRMClient.ContainerRequest request) {
    MockContainer allocated = allocator.allocate(request)
    if (allocated != null) {
      MockContainerId id = allocated.id as MockContainerId
      id.applicationAttemptId = attemptId;
    }
    return allocated
  }

  boolean releaseContainer(ContainerId containerId) {
    cluster.release(containerId)
  }

  /**
   * Process a list of operations -release containers to be released,
   * allocate those for which there is space (but don't rescan the list after
   * the scan)
   * @param ops
   * @return
   */
  List<Container> process(List<AbstractRMOperation> ops) {
    def (allocation, remainder) = processQueue(ops)
    return allocation
  }

  /**
   * Process a list of operations -release containers to be released,
   * allocate those for which there is space (but don't rescan the list after
   * the scan)
   * @param ops
   * @return
   */
  def processQueue(List<AbstractRMOperation> ops) {
    List<Container> allocation = [];
    List<ContainerRequestOperation> unsatisfiedAllocations = []
    ops.each { AbstractRMOperation op ->
      if (op instanceof ContainerReleaseOperation) {
        ContainerReleaseOperation cro = (ContainerReleaseOperation) op
        releaseContainer(cro.containerId);
      } else {
        ContainerRequestOperation req = (ContainerRequestOperation) op
        Container container = allocateContainer(req.request)
        if (container != null) {
          allocation.add(container)
        } else {
          unsatisfiedAllocations.add(req)
        }
      }
    }
    return [allocation, unsatisfiedAllocations]
  }

}