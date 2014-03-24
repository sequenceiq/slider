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
package org.apache.hoya.yarn.appmaster.web.rest;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hoya.yarn.appmaster.web.WebAppApi;
import org.apache.hoya.yarn.appmaster.web.rest.agent.AgentResource;
import org.apache.hoya.yarn.appmaster.web.rest.management.ManagementResource;

import javax.ws.rs.*;

/** The available REST services exposed by a slider AM. */
@Singleton
@Path(AMWebServices.WS_CONTEXT_ROOT)
public class AMWebServices {
  public static final String WS_CONTEXT_ROOT = "/ws/v1/slider";
  /** AM/WebApp info object */
  private WebAppApi slider;

  @Inject
  public AMWebServices(WebAppApi slider) {
    this.slider = slider;
  }

  @Path("/mgmt")
  public ManagementResource getManagementResource() {
    return new ManagementResource(slider);
  }

  @Path("/agents")
  public AgentResource getAgentResource () {
    return new AgentResource(slider);
  }

}
