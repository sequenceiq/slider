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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hoya.providers.ProviderRole;
import org.apache.hoya.providers.ProviderService;
import org.apache.hoya.yarn.appmaster.state.AppState;
import org.apache.hoya.yarn.appmaster.state.RoleStatus;
import org.apache.hoya.yarn.appmaster.web.WebAppApi;

import com.google.inject.Inject;

/**
 * 
 */
public class ContainerStatsBlock extends HtmlBlock {

  private AppState appState;
  private ProviderService providerService;
  
  @Inject
  public ContainerStatsBlock(WebAppApi hoya) { 
    this.appState = hoya.getAppState();
    this.providerService = hoya.getProviderService();
  }

  @Override
  protected void render(Block html) {
    Hamlet hamlet = html.h1("Cluster Statistics");
    
    Map<Integer,ProviderRole> rolesById = rolesById(providerService.getRoles());
    Map<Integer,RoleStatus> status = appState.getRoleStatusMap();
    for (Entry<Integer,RoleStatus> entry : status.entrySet()) {
      ProviderRole role = rolesById.get(entry.getKey());
      
      // If we don't have a role from the provider, assume it's the Hoya AM
      String name = "Hoya Application Master";
      if (null != role) {
        name = role.name;
      }
      
      hamlet.div().
      h3(name).
      p().
      _(entry.getValue().toString())._()._();
    }
  }

  private Map<Integer,ProviderRole> rolesById(List<ProviderRole> roles) {
    Map<Integer,ProviderRole> rolesById = new HashMap<Integer,ProviderRole>();
    for (ProviderRole role : roles) {
      rolesById.put(role.id, role);
    }
    
    return rolesById;
  }
  
}
