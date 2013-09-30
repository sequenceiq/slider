/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hoya.yarn.appmaster.state;

import org.apache.hadoop.hoya.api.ClusterDescription;
import org.apache.hadoop.hoya.api.ClusterNode;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;

import java.util.UUID;

/**
 * Info about a continer to keep around when deciding which container to release
 */
public class ContainerInfo {

  public Container container;
  /**
   * UUID of container used in Hoya RPC to refer to instances
   */
  public String uuid;
  public long createTime;
  public long startTime;
  /**
   * flag set when it is released, to know if it has
   * already been targeted for termination
   */
  public boolean released;
  public String role;
  public int roleId;
  /**
   * state from {@link ClusterDescription}
   */
  public int state;

  /**
   * Exit code: only valid if the state >= STOPPED
   */
  public int exitCode;

  /**
   * what was the command executed?
   */
  public String command;

  /**
   * Any diagnostics
   */
  public String diagnostics;

  /**
   * What is the tail output from the executed process (or [] if not started
   * or the log cannot be picked up
   */
  public String[] output;

  /**
   * Any environment details
   */
  public String[] environment;

  public ContainerInfo(Container container) {
    this.container = container;
  }

  public ContainerId getId() {
    return container.getId();
  }
  
  public NodeId getNodeId() {
    return container.getNodeId();
  }
  
  @Override
  public String toString() {
    return "ContainerInfo{" +
           "container=" + container +
           ", role='" + role + '\'' +
           '}';
  }

  public void buildUUID() {
    uuid = UUID.randomUUID().toString();
  }
  public ContainerId getContainerId() {
    return container!=null? container.getId(): null;
  }
  /**
   * Build the wire representation for status messages
   * @return an instance to send back over the network
   */
  public ClusterNode toWireFormat() {
    ClusterNode node;
    if (container != null) {
      node = new ClusterNode(container.getId());
    } else {
      node = new ClusterNode();
      node.name = "unallocated instance";
    }
    node.role = role;
    node.roleId = roleId;
    node.state = state;
    node.exitCode = exitCode;
    node.command = command;
    node.diagnostics = diagnostics;
    node.output = output;
    node.environment = environment;
    node.uuid = uuid;
    return node;
  }
}
