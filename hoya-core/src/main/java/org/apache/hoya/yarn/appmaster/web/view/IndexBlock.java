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

import com.google.inject.Inject;
import java.net.URL;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.UL;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hoya.api.StatusKeys;
import org.apache.hoya.providers.ProviderService;
import org.apache.hoya.yarn.appmaster.state.AppState;
import org.apache.hoya.yarn.appmaster.web.WebAppApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class IndexBlock extends HtmlBlock {
  private static final String HBASE = "HBase";
  private static final Logger log = LoggerFactory.getLogger(IndexBlock.class);

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

    doIndex(html, providerName);
  }

  // An extra method to make testing easier since you can't make an instance of Block
  protected void doIndex(Hamlet html, String providerName) {
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
    addProviderServiceOptions(providerService, ul);
    ul._()._();
  }

  private String getProviderName() {
    String providerServiceName = providerService.getName().toLowerCase();

    // Get HBase properly capitalized
    if (providerServiceName.contains("hbase")) {
      return HBASE;
    }

    return StringUtils.capitalize(providerServiceName);
  }

  private String getInfoAvoidingNulls(String key) {
    String createTime = appState.clusterDescription.getInfo(key);

    return null == createTime ? "N/A" : createTime;
  }

  protected void addProviderServiceOptions(ProviderService providerService, UL<DIV<Hamlet>> ul) {
    Map<String,URL> details = providerService.buildMonitorDetails(appState.clusterDescription);
    if (null == details) {
      return;
    }
    
    // Loop over each entry, placing the text in the UL, adding an anchor when the URL is non-null
    for (Entry<String,URL> entry : details.entrySet()) {
      if (null != entry.getValue()) {
        String url = entry.getValue().toString();
        ul.li()._(entry.getKey()).a(url, url)._();
      } else {
        ul.li(entry.getKey());
      }
    }
  }

}
