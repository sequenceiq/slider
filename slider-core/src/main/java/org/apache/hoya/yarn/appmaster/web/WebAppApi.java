/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hoya.yarn.appmaster.web;

import java.util.Map;

import org.apache.hoya.api.HoyaClusterProtocol;
import org.apache.hoya.providers.ProviderService;
import org.apache.hoya.yarn.appmaster.state.AppState;
import org.apache.hoya.yarn.appmaster.state.RoleStatus;
import org.apache.hoya.yarn.appmaster.state.StateAccessForProviders;
import org.apache.hoya.yarn.appmaster.web.rest.agent.AgentRestOperations;

/**
 * Interface to pass information from the Hoya AppMaster to the WebApp
 */
public interface WebAppApi {

  /**
   * The {@link AppState} for the current cluster
   */
  public StateAccessForProviders getAppState();
  
  /**
   * The {@link ProviderService} for the current cluster
   */
  public ProviderService getProviderService();
  
  /**
   * The {@link HoyaClusterProtocol} for the current cluster
   */
  public HoyaClusterProtocol getClusterProtocol();
  
  /**
   * Generate a mapping from role name to its {@link RoleStatus}. Be aware that this
   * is a computed value and not just a getter
   */
  public Map<String,RoleStatus> getRoleStatusByName();

  /**
   * Returns an interface that can support the agent-based REST operations.
   */
  public AgentRestOperations getAgentRestOperations();
}
