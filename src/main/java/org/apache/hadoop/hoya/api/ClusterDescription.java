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

package org.apache.hadoop.hoya.api;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a cluster specification; designed to be sendable over the wire
 * and persisted in JSON by way of Jackson.
 * As a wire format it is less efficient in both xfer and ser/deser than 
 * a binary format, but by having one unified format for wire and persistence,
 * the code paths are simplified.
 */
public class ClusterDescription {

  public String name;
  public int state;
  
  /*
   State list for both clusters and nodes in them. Ordered so that destroyed follows
   stopped
   */
  public static final int STATE_CREATED = 0;
  public static final int STATE_LIVE = 1;
  public static final int STATE_STOPPED = 2;
  public static final int STATE_DESTROYED = 3;
  /**
   * When was the cluster last started?
   */
  public long startTime;

  /**
   * When was the cluster last stopped: should be 0 if the cluster is live
   */
  public long stopTime;
  /**
   * when was this status document created
   */
  public long statusTime;

  /**
   * The path to the original configuration
   * files; these are re-read when 
   * restoring a cluster
   */
  
  public String originConfigurationPath;
  public int minRegionNodes;
  public int maxRegionNodes;
  public int minMasterNodes;
  public int maxMasterNodes;

  public String zkHosts;
  public int zkPort;
  public String zkPath;
  public String hBaseRootPath;
  /**
   * Master node
   */
  public List<ClusterNode> masterNodes = new ArrayList<ClusterNode>();
  /**
   *  Worker nodes
   */
  public List<ClusterNode> regionNodes = new ArrayList<ClusterNode>();

  /**
   *  Completed nodes
   */
  public List<ClusterNode> completedNodes = new ArrayList<ClusterNode>();

  
  /**
   * List of key-value pairs to add to an HBase config to set up the client
   */
  public Map<String,String> hBaseClientProperties = new HashMap<String, String>();

  /**
   * Describe a specific node in the cluster
   */
  public static class ClusterNode {
    /**
     * server name
     */
    public String name;
    /**
     * state (Currently arbitrary text)
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
    
    public ClusterNode(String name) {
      this.name = name;
    }

    public ClusterNode() {
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append(name).append(": ").append(state).append("\n");
      builder.append(command).append("\n");
      for (String line : output) {
        builder.append(line).append("\n");
      }
      if (diagnostics != null) {
        builder.append(diagnostics).append("\n");
      }
      return builder.toString();
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("Hoya Cluster ").append(name).append('\n');
    builder.append("State: ").append(state).append('\n');
    if (startTime > 0) {
      builder.append("Started: ");
      builder.append(new Date(startTime).toLocaleString());
    }
    if (stopTime > 0) {
      builder.append("Stopped: ");
      builder.append(new Date(startTime).toLocaleString());
    }
    builder.append("RS nodes: ")
           .append(minRegionNodes)
           .append('-')
           .append(maxRegionNodes)
           .append('\n');
    builder.append("ZK cluster: ").append(zkHosts).
      append(" : ").append(zkPort).append('\n');
    builder.append("ZK path: ").append(zkPath).append('\n');
    builder.append(String.format("HBase Master count %d", masterNodes.size()));
    for (ClusterNode node : masterNodes) {
      builder.append("    ");
      builder.append(node.toString()).append('\n');
    }
    builder.append(String.format("Region Server count %d", regionNodes.size()));
    for (ClusterNode node : regionNodes) {
      builder.append("    ");
      builder.append(node.toString()).append('\n');
    }
    return builder.toString();
  }

  /**
   * Convert to a JSON string
   * @return a JSON string description
   * @throws IOException Problems mapping/writing the object
   */
  public String toJsonString() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.writeValueAsString(this);
  }
  
  public static ClusterDescription fromJson(String json) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    ClusterDescription cd = mapper.readValue(json, ClusterDescription.class);
    return cd;
  }
}
