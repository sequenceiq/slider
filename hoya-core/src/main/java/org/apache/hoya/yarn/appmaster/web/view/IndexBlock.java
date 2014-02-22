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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.UL;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hoya.api.StatusKeys;
import org.apache.hoya.providers.ProviderService;
import org.apache.hoya.providers.accumulo.AccumuloKeys;
import org.apache.hoya.providers.accumulo.AccumuloProviderService;
import org.apache.hoya.providers.hbase.HBaseProviderService;
import org.apache.hoya.yarn.appmaster.state.AppState;
import org.apache.hoya.yarn.appmaster.web.WebAppApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

/**
 * 
 */
public class IndexBlock extends HtmlBlock {
  private static final String ACCUMULO = "Accumulo", HBASE = "HBase", UNKNOWN = "Unknown";
  private static final Logger log = LoggerFactory.getLogger(IndexBlock.class);

  // Public for tests to use for verification
  public static final String HBASE_MASTER_ADDR_LABEL = "Active HBase Master (RPC): ", ACCUMULO_MASTER_ADDR_LABEL = "Active Accumulo Master (RPC): ",
      ACCUMULO_MONITOR_ADDR_LABEL = "Active Accumulo Monitor: ";

  private AppState appState;
  private ProviderService providerService;

  @Inject
  public IndexBlock(WebAppApi hoya) {
    this.appState = hoya.getAppState();
    this.providerService = hoya.getProviderService();
  }

  @Override
  protected void render(Block html) {
    final String providerName = getProviderName();

    DIV<Hamlet> div = html.div("general_info").h1("index_header", providerName + " cluster: '" + appState.clusterDescription.name + "'");

    UL<DIV<Hamlet>> ul = div.ul();

    ul.li("Total number of containers for cluster: " + appState.getNumActiveContainers());
    ul.li("Cluster created: " + getInfoAvoidingNulls(StatusKeys.INFO_CREATE_TIME_HUMAN));
    ul.li("Cluster last flexed: " + getInfoAvoidingNulls(StatusKeys.INFO_FLEX_TIME_HUMAN));
    ul.li("Cluster running since: " + getInfoAvoidingNulls(StatusKeys.INFO_LIVE_TIME_HUMAN));
    ul.li("Cluster HDFS storage path: " + appState.clusterDescription.dataPath);
    ul.li("Cluster configuration path: " + appState.clusterDescription.originConfigurationPath);

    ul._()._();

    html.div("provider_info").h3(providerName + " specific information");
    ul = div.ul();
    addProviderSpecificOptions(ul);
    ul._()._();
  }

  private String getProviderName() {
    String providerServiceName = providerService.getName().toLowerCase();

    if (providerServiceName.contains("accumulo")) {
      return ACCUMULO;
    } else if (providerServiceName.contains("hbase")) {
      return HBASE;
    }

    return UNKNOWN;
  }

  private String getInfoAvoidingNulls(String key) {
    String createTime = appState.clusterDescription.getInfo(key);

    return null == createTime ? "N/A" : createTime;
  }

  private void addProviderSpecificOptions(UL<DIV<Hamlet>> ul) {
    Class<?> clz = providerService.getClass();
    if (AccumuloProviderService.class.equals(clz)) {
      addAccumuloProviderOptions((AccumuloProviderService) providerService, ul);
    } else if (HBaseProviderService.class.equals(clz)) {
      addHBaseProviderOptions((HBaseProviderService) providerService, ul);
    } else {
      log.debug("Could not determine provider service for class {} ", clz);
    }
  }

  private void addAccumuloProviderOptions(AccumuloProviderService accProviderService, UL<DIV<Hamlet>> ul) {
    ul.li(ACCUMULO_MASTER_ADDR_LABEL + getInfoAvoidingNulls(AccumuloKeys.MASTER_ADDRESS));

    String monitorAddr = appState.clusterDescription.getInfo(AccumuloKeys.MONITOR_ADDRESS);
    if (!StringUtils.isBlank(monitorAddr)) {
      ul.li()._(ACCUMULO_MONITOR_ADDR_LABEL).a("http://" + monitorAddr, monitorAddr)._();
    } else
      ul.li(ACCUMULO_MONITOR_ADDR_LABEL + "N/A");
  }

  private void addHBaseProviderOptions(HBaseProviderService hbaseProviderService, UL<DIV<Hamlet>> ul) {
    ul.li(HBASE_MASTER_ADDR_LABEL + getInfoAvoidingNulls(StatusKeys.INFO_MASTER_ADDRESS));
  }

}
