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
package org.apache.hoya.yarn.appmaster.web.view;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.UL;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hoya.HoyaKeys;
import org.apache.hoya.api.ClusterNode;
import org.apache.hoya.providers.ProviderRole;
import org.apache.hoya.providers.ProviderService;
import org.apache.hoya.yarn.appmaster.state.AppState;
import org.apache.hoya.yarn.appmaster.state.RoleStatus;
import org.apache.hoya.yarn.appmaster.web.WebAppApi;
import org.apache.hoya.yarn.client.HoyaClusterOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

/**
 * 
 */
public class ContainerStatsBlock extends HtmlBlock {
  private static final Logger log = LoggerFactory.getLogger(ContainerStatsBlock.class);
  private static final ProviderRole HOYA_AM_ROLE = new ProviderRole("Hoya Application Master", HoyaKeys.ROLE_HOYA_AM_PRIORITY_INDEX);
  
  private AppState appState;
  private ProviderService providerService;
  private HoyaClusterOperations clusterOps;
  
  @Inject
  public ContainerStatsBlock(WebAppApi hoya) { 
    this.appState = hoya.getAppState();
    this.providerService = hoya.getProviderService();
    this.clusterOps = new HoyaClusterOperations(hoya.getClusterProtocol());
  }

  @Override
  protected void render(Block html) {
    Hamlet hamlet = html.h1("Cluster Statistics");
    
    Map<Integer,ProviderRole> rolesById = rolesById(providerService.getRoles());
    Map<Integer,RoleStatus> status = appState.getRoleStatusMap();
    for (Entry<Integer,RoleStatus> entry : status.entrySet()) {
      ProviderRole role = rolesById.get(entry.getKey());

      if (null == role) {
        log.error("Found ID ({}) which has no known ProviderRole", entry.getKey());
      }
      
      String name = role.name;
      
      DIV<Hamlet> div = hamlet.div();
      div.p(entry.getValue().toString());
      
      List<ClusterNode> nodesInRole;
      try {
        nodesInRole = clusterOps.listClusterNodesInRole(name);
      } catch (Exception e) {
        log.error("Could not fetch nodes for role: " + name, e);
        nodesInRole = Collections.emptyList();
      }
      
      UL<DIV<Hamlet>> ul = div.h3(name).ul();
      for (ClusterNode node : nodesInRole) {
        ul.li()._(node.name, " ", node.diagnostics)._();
      }
      
      ul._();
      div._();
    }
  }

  private Map<Integer,ProviderRole> rolesById(List<ProviderRole> roles) {
    Map<Integer,ProviderRole> rolesById = new HashMap<Integer,ProviderRole>();
    rolesById.put(HoyaKeys.ROLE_HOYA_AM_PRIORITY_INDEX, HOYA_AM_ROLE);
    
    for (ProviderRole role : roles) {
      rolesById.put(role.id, role);
    }
    
    return rolesById;
  }
  
}
