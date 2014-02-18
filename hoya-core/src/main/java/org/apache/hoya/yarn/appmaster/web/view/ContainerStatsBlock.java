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

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.P;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hoya.HoyaKeys;
import org.apache.hoya.api.ClusterDescription;
import org.apache.hoya.api.ClusterNode;
import org.apache.hoya.providers.ProviderRole;
import org.apache.hoya.providers.ProviderService;
import org.apache.hoya.yarn.appmaster.state.AppState;
import org.apache.hoya.yarn.appmaster.state.RoleStatus;
import org.apache.hoya.yarn.appmaster.web.WebAppApi;
import org.apache.hoya.yarn.client.HoyaClusterOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

/**
 * 
 */
public class ContainerStatsBlock extends HtmlBlock {
  private static final Logger log = LoggerFactory.getLogger(ContainerStatsBlock.class);
  private static final ProviderRole HOYA_AM_ROLE = new ProviderRole("Hoya Application Master", HoyaKeys.ROLE_HOYA_AM_PRIORITY_INDEX);
  private static final String EVEN = "even", ODD = "odd", BOLD = "bold";

  private AppState appState;
  private ProviderService providerService;
  private HoyaClusterOperations clusterOps;

  @Inject
  public ContainerStatsBlock(WebAppApi hoya) {
    this.appState = hoya.getAppState();
    this.providerService = hoya.getProviderService();
    this.clusterOps = new HoyaClusterOperations(hoya.getClusterProtocol());
  }

  private static class ClusterNodeNameComparator implements Comparator<ClusterNode> {

    @Override
    public int compare(ClusterNode node1, ClusterNode node2) {
      if (null == node1 && null != node2) {
        return -1;
      } else if (null != node1 && null == node2) {
        return 1;
      } else if (null == node1 && null == node2) {
        return 0;
      }

      final String name1 = node1.name, name2 = node2.name;
      if (null == name1 && null != name2) {
        return -1;
      } else if (null != name1 && null == name2) {
        return 1;
      } else if (null == name1 && null == name2) {
        return 0;
      }

      return name1.compareTo(name2);
    }

  }

  @Override
  protected void render(Block html) {
    // TODO Move this garbage to the controller and inject it into this block
    Map<Integer,ProviderRole> rolesById = rolesById(providerService.getRoles());
    Map<Integer,RoleStatus> status = appState.getRoleStatusMap();
    Map<String,RoleStatus> roleNameToStatus = getRoleStatusesByName(rolesById, status);

    for (Entry<String,RoleStatus> entry : roleNameToStatus.entrySet()) {
      final String name = entry.getKey();
      final RoleStatus roleStatus = entry.getValue();

      DIV<Hamlet> div = html.div("role-info ui-widget-content ui-corner-all");

      List<ClusterNode> nodesInRole;
      try {
        nodesInRole = clusterOps.listClusterNodesInRole(name);
      } catch (Exception e) {
        log.error("Could not fetch containers for role: " + name, e);
        nodesInRole = Collections.emptyList();
      }

      div.h2(BOLD, StringUtils.capitalize(name));

      TABLE<DIV<DIV<Hamlet>>> table = div.div("role-stats-wrap").h3(BOLD, "Specifications").table("role-stats ui-widget-content ui-corner-bottom");

      TBODY<TABLE<DIV<DIV<Hamlet>>>> tbody = table.tbody();

      tbody.tr(EVEN).td("Desired").td(Integer.toString(roleStatus.getDesired()))._();
      tbody.tr(ODD).td("Actual").td(Integer.toString(roleStatus.getActual()))._();
      tbody.tr(EVEN).td("Requested").td(Integer.toString(roleStatus.getRequested()))._();
      tbody.tr(ODD).td("Releasing").td(Integer.toString(roleStatus.getReleasing()))._();
      tbody.tr(EVEN).td("Failed").td(Integer.toString(roleStatus.getFailed()))._();
      tbody.tr(ODD).td("Start Failed").td(Integer.toString(roleStatus.getStartFailed()))._();
      tbody.tr(EVEN).td("Completed").td(Integer.toString(roleStatus.getCompleted()))._();

      tbody._()._()._();

      // Sort the ClusterNodes by their name (containerid)
      Collections.sort(nodesInRole, new ClusterNodeNameComparator());

      DIV<DIV<Hamlet>> roleStatsContainers = div.div("role-stats-containers").h3(BOLD, "Containers");

      if (!nodesInRole.isEmpty()) {
        table = roleStatsContainers.table("ui-widget-content ui-corner-bottom");
        tbody = table.tbody();

        int offset = 0;
        for (ClusterNode node : nodesInRole) {
          tbody.tr(offset % 2 == 0 ? EVEN : ODD).td(node.name)._();
          offset++;
        }

        tbody._()._()._();
      } else {
        // TODO we should be able to the get the AM container by normal means
        roleStatsContainers.p("no-table-contents")._("None")._()._();
      }

      ClusterDescription desc = appState.clusterDescription;
      Map<String,String> options = desc.getRole(name);

      DIV<DIV<Hamlet>> roleOptionsWrap = div.div("role-options-wrap").h3(BOLD, "Role Options");

      if (null != options) {
        table = roleOptionsWrap.table("ui-widget-content ui-corner-bottom");
        tbody = table.tbody();

        int offset = 0;
        for (Entry<String,String> option : options.entrySet()) {
          tbody.tr(offset % 2 == 0 ? EVEN : ODD).td(option.getKey()).td(option.getValue())._();
          offset++;
        }

        tbody._()._()._();
      } else {
        roleOptionsWrap.p("no-table-contents")._("None")._()._();
      }

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

  private TreeMap<String,RoleStatus> getRoleStatusesByName(Map<Integer,ProviderRole> rolesById, Map<Integer,RoleStatus> statusById) {
    TreeMap<String,RoleStatus> statusByName = new TreeMap<String,RoleStatus>();
    for (Entry<Integer,ProviderRole> role : rolesById.entrySet()) {
      final RoleStatus status = statusById.get(role.getKey());

      if (null == status) {
        log.error("Found ID ({}) which has no known ProviderRole", role.getKey());
      } else {
        statusByName.put(role.getValue().name, status);
      }
    }

    return statusByName;
  }

}
