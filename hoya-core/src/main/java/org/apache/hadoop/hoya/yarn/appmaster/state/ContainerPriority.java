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

import org.apache.hadoop.yarn.api.records.Container;

/**
 * Class containing the logic to build/split container priorities into the
 * different fields used by Hoya
 *
 The container priority field (a 32 bit integer) is used by Hoya
 to index the specific role in a container so as to determine which role
 has been offered in a container allocation message, and which role has
 been released on a release event.

 The Role History needs to track outstanding requests, so that
 when an allocation comes in, it can be mapped back to the original request.
 Simply looking up the nodes on the provided container and decrementing
 its request counter is not going to work -the container may be allocated
 on a different node from that requested.

 The priority field of a request is divided by Hoya into 8 bits for
 `roleID` and 24 bits for `requestID`. The request ID will be a simple
 rolling integer -Hoya will assume that after 2^24 requests per role, it can be rolled,
 -though as we will be retaining a list of outstanding requests, a clash should not occur.
 The main requirement  is: not have more than 2^24 outstanding requests for instances of a specific role,
 which places an upper bound on the size of a Hoya cluster.

 */
public final class ContainerPriority {

  public static int buildPriority(int role, int requestId) {
    return role | (requestId << 24);
  }
  public static int extractRole(int priority) {
    return priority & 0x7;
  }

  /**
   * Get the request ID from the priority
   * @param priority the priority field
   * @return an extracted (rotating and masking)
   */
  public static int extractRequestId(int priority) {
    return (priority >>> 24 ) & 0x0fff;
  }

  /**
   * Map from a container to a role key by way of its priority
   * @param container container
   * @return role key
   */
  public static int extractRole(Container container) {
    return extractRole(container.getPriority().getPriority());
  }

  /**
   * Map from a container to a role key by way of its priority
   * @param container container
   * @return the request ID
   */
  public static int extractRequestId(Container container) {
    return extractRequestId(container.getPriority().getPriority());
  }
}
