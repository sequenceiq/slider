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

import java.util.List;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.P;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hoya.api.ClusterDescription;
import org.apache.hoya.providers.ProviderService;
import org.apache.hoya.yarn.appmaster.state.AppState;
import org.apache.hoya.yarn.appmaster.web.WebAppApi;

import com.google.inject.Inject;

/**
 * 
 */
public class IndexBlock extends HtmlBlock {
  private static final String ACCUMULO = "Accumulo", HBASE = "HBase", UNKNOWN = "Unknown";

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
    
    ClusterDescription desc = appState.clusterDescription;
    
    long totalProcs = 0;
    for (Entry<String,List<String>> entry : desc.instances.entrySet()) {
      if (null != entry.getValue()) {
        totalProcs += entry.getValue().size();
      }
    }
    
    Hamlet h = html.h1("Hoya deployed " + providerName + " cluster: " + desc.name);
    
    h.p()._("Number of services: ", Long.toString(totalProcs))._();
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

}
