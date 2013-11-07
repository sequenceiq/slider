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

package org.apache.hadoop.hoya.yarn.appmaster.state;


import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks an outstanding request. This is used to correlate an allocation response
 (whose Container Priority file is used to locate this request), with the
 node and role used in the request.

 roleId:  int
 requestID :  int
 node: string (may be null)
 requestedTime: long
 priority: int = requestID << 24 | roleId

 The node identifier may be null -which indicates that a request was made without
 a specific target node

 Equality and the hash code are based <i>only</i> on the request ID; which
 is fixed at construction time.
 */
public final class OutstandingRequest {
  protected static final Logger log =
    LoggerFactory.getLogger(OutstandingRequest.class);

  public final int roleId;
  public final int requestID;

  public NodeInstance node;
  public long requestedTime;

  public OutstandingRequest(int roleId,
                            int requestID,
                            NodeInstance node) {
    this.roleId = roleId;
    this.requestID = requestID;
    this.node = node;
  }

  /**
   * Build a container request.
   * If the request has an address, it is set in the container request
   * (with a flag to enable relaxed priorities)
   * @param resource resource
   * @return the request to raise
   */
  public AMRMClient.ContainerRequest buildContainerRequest(Resource resource) {
    String[] hosts;
    boolean relaxLocality;
    boolean locationSpecified;
    if (node != null) {
      hosts = new String[1];
      hosts[0] = node.hostname;
      relaxLocality = true;
      locationSpecified = true;
      // tell the node it is in play
      node.getOrCreate(roleId);
      log.info("Submitting request for container on {}", hosts[0]);
    } else {
      hosts = null;
      relaxLocality = true;
      locationSpecified = false;
    }
    Priority pri = ContainerPriority.createPriority(roleId,
                                                    requestID,
                                                    locationSpecified);
    AMRMClient.ContainerRequest request =
      new AMRMClient.ContainerRequest(resource,
                                      hosts,
                                      null,
                                      pri,
                                      relaxLocality);
    
    return request;
  }
  
  
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    OutstandingRequest that = (OutstandingRequest) o;
    return requestID == that.requestID;
  }

  @Override
  public int hashCode() {
    return requestID;
  }

  @Override
  public String toString() {
    final StringBuilder sb =
      new StringBuilder("OutstandingRequest{");
    sb.append("roleId=").append(roleId);
    sb.append(", requestID=").append(requestID);
    sb.append(", node='").append(node).append('\'');
    sb.append(", requestedTime=").append(requestedTime);
    sb.append('}');
    return sb.toString();
  }
}
