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

import java.util.Map.Entry;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.P;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hoya.api.ClusterDescription;
import org.apache.hoya.yarn.appmaster.state.AppState;
import org.apache.hoya.yarn.appmaster.web.WebAppApi;

import com.google.inject.Inject;

/**
 * 
 */
public class IndexBlock extends HtmlBlock {

  private AppState appState;
  
  @Inject
  public IndexBlock(WebAppApi hoya) { 
    this.appState = hoya.getAppState();
  }
  
  @Override
  protected void render(Block html) {
    Hamlet h = html.h1("Hoya App Master").h2("Roles");
    
    ClusterDescription desc = appState.clusterDescription;
    for (String roleName : desc.getRoleNames()) {
      P<Hamlet> p = h.h3(roleName)
      .p();
      for (Entry<String,String> option : desc.getRole(roleName).entrySet()) {
        p._(option.getKey(), " ", option.getValue());
      }
      p._();
    }
  }

}
