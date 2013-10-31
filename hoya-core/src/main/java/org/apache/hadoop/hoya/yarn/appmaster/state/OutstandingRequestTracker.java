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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Contains a map from requestID to the specific `OutstandingRequest` made.
 * 
 * Not all requests have locations; they still have request IDs and are still
 * tracked
 */

public class OutstandingRequestTracker {
  protected static final Logger log =
    LoggerFactory.getLogger(OutstandingRequestTracker.class);
  private int nextRequestId = 1;


  private Map<Integer, OutstandingRequest> requests =
    new HashMap<Integer, OutstandingRequest>();

  /*
     : OutstandingRequest
        (and an updated request Map with a new entry)
    lookup(RequestID): OutstandingRequest
    remove(RequestID): OutstandingRequest
    listRequestsForNode(NodeID): [OutstandingRequest]
   */

  /**
   * Create a new request (with a new request ID) for the specific role.
   * This does not update the node instance's role's request count
   * @param instance node instance to manager
   * @param role role index
   * @return a new request
   */
  public OutstandingRequest addRequest(NodeInstance instance, int role) {
    int id = nextRequestId++;
    if (nextRequestId == 0) {
      //wraparound
      log.warn("Request ID counter has just wrapped around");
      //skip the special "0" id
      nextRequestId = 1;
    }
    OutstandingRequest request =
      new OutstandingRequest(role, id, instance);
    requests.put(id, request);
    return request;
  }

  public OutstandingRequest lookup(int requestID) {
    return requests.get(requestID);
  }

  public OutstandingRequest remove(int requestID) {
    return requests.remove(requestID);
  }


}
