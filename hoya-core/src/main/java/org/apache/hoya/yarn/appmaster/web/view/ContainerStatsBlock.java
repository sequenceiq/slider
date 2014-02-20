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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TR;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hoya.api.ClusterDescription;
import org.apache.hoya.api.ClusterNode;
import org.apache.hoya.yarn.appmaster.state.RoleStatus;
import org.apache.hoya.yarn.appmaster.web.WebAppApi;
import org.apache.hoya.yarn.client.HoyaClusterOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

/**
 * 
 */
public class ContainerStatsBlock extends HtmlBlock {
  private static final Logger log = LoggerFactory.getLogger(ContainerStatsBlock.class);
  
  private static final String EVEN = "even", ODD = "odd", BOLD = "bold";

  private WebAppApi hoya;
  private HoyaClusterOperations clusterOps;
  
  @Inject
  public ContainerStatsBlock(WebAppApi hoya) {
    this.hoya = hoya;
    clusterOps = new HoyaClusterOperations(hoya.getClusterProtocol());
  }

  /**
   * Sort a collection of ClusterNodes by name
   */
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

    for (Entry<String,RoleStatus> entry : hoya.getRoleStatusByName().entrySet()) {
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

      // Generate the details on this role
      generateRoleDetails(div.div("role-stats-wrap"), "Specifications", roleStatus.buildStatistics().entrySet());

      // Sort the ClusterNodes by their name (containerid)
      Collections.sort(nodesInRole, new ClusterNodeNameComparator());

      // Generate the containers running this role
      generateRoleDetails(div.div("role-stats-containers"), "Containers", Iterables.transform(nodesInRole, new Function<ClusterNode,Entry<String,String>>() {

        @Override
        public Entry<String,String> apply(ClusterNode input) {
          return Maps.immutableEntry(input.name, null);
        }
        
      }));

      ClusterDescription desc = hoya.getAppState().clusterDescription;
      Map<String,String> options = desc.getRole(name);

      // Generate the options used by this role
      generateRoleDetails(div.div("role-options-wrap"), "Role Options", (null != options) ? options.entrySet() : Collections.<Entry<String,String>> emptySet());

      div._();
    }
  }
  
  /**
   * Given a div, a name for this data, and some pairs of data, generate a nice HTML table. If contents
   * is empty (of size zero), then a mesage will be printed that there were no items instead of an empty table.
   * @param div
   * @param detailsName
   * @param contents
   */
  private <T> void generateRoleDetails(DIV<DIV<Hamlet>> div, String detailsName, Iterable<Entry<String,T>> contents) {
    div.h3(BOLD, detailsName);
    
    int offset = 0;
    TABLE<DIV<DIV<Hamlet>>> table = null;
    TBODY<TABLE<DIV<DIV<Hamlet>>>> tbody = null;
    for (Entry<String,T> content : contents) {
      if (null == table) {
        table = div.table("ui-widget-content ui-corner-bottom");
        tbody = table.tbody();
      }
      
      TR<TBODY<TABLE<DIV<DIV<Hamlet>>>>> row = tbody.tr(offset % 2 == 0 ? EVEN : ODD).td(content.getKey());
      
      // Only add the second column if the element is non-null
      // This also lets us avoid making a second method if we're only making a one-column table
      if (null != content.getValue()) {
        row.td(content.getValue().toString());
      }
      
      row._();
      
      offset++;
    }
    
    // If we made a table, close it out
    if (null != table) {
      tbody._()._()._();
    } else {
      // Otherwise, throw in a nice "no content" message
      div.p("no-table-contents")._("None")._()._();
    }
  }

}
