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

import groovy.util.logging.Slf4j
import org.apache.hadoop.hoya.yarn.appmaster.state.AbstractRMOperation
import org.apache.hadoop.hoya.yarn.appmaster.state.ContainerReleaseOperation
import org.apache.hadoop.hoya.yarn.appmaster.state.ContainerRequestOperation
import org.apache.hadoop.hoya.yarn.appmaster.state.RMOperationHandler
import org.apache.hadoop.yarn.api.records.ContainerId
import org.apache.hadoop.yarn.client.api.AMRMClient


@Slf4j
class MockRMOperationHandler extends RMOperationHandler {
  List<AbstractRMOperation> operations = [];
  
  @Override
  void releaseAssignedContainer(ContainerId containerId) {
    operations.add(new ContainerReleaseOperation(containerId))
    log.info("Releasing container ID " + containerId.getId())
  }

  @Override
  void addContainerRequest(AMRMClient.ContainerRequest req) {
    operations.add(new ContainerRequestOperation(req))
    log.info("Requesting container role #" + req.priority);

  }
}
