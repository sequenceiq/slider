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

import org.apache.hadoop.yarn.webapp.Controller;
import org.apache.hoya.yarn.appmaster.web.layout.AppLayout;
import org.apache.hoya.yarn.appmaster.web.layout.ContainerStatsView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

/**
 * 
 */
public class HoyaAMController extends Controller {
  private static final Logger log = LoggerFactory.getLogger(HoyaAMController.class);

  private final WebAppApi hoya;
  
  @Inject
  public HoyaAMController(WebAppApi hoya, RequestContext ctx) {
    super(ctx);
    this.hoya = hoya;
  }
  
  @Override
  public void index() {
    setTitle("Hoya App Master");
    
    updateAppState();
    
    render(AppLayout.class);
  }
  
  public void containerStats() {
    setTitle("Hoya Container Statistics");
    
    updateAppState();
    
    render(ContainerStatsView.class);
  }

  private void updateAppState() {
    //TODO don't do this on every request?
    Map<String,String> providerStatus = hoya.getProviderService().buildProviderStatus();
    
    if (null != providerStatus) {
      log.info("Updating AppState with status from provider: {}", providerStatus.toString());
      
      hoya.getAppState().refreshClusterStatus(providerStatus);
    }
  }
  
}
