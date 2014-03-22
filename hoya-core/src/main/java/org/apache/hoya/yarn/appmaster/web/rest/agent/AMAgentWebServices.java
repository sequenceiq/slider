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
package org.apache.hoya.yarn.appmaster.web.rest.agent;

import com.google.inject.Inject;
import org.apache.hoya.yarn.appmaster.web.WebAppApi;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

/** The available REST services exposed by a slider AM. */
public class AMAgentWebServices {
  /** AM/WebApp info object */
  private WebAppApi slider;

  @Inject
  public AMAgentWebServices(WebAppApi slider) {
    this.slider = slider;
  }

  @POST
  @Path("/agent/register/{agent_name}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public RegistrationResponse register(Register registration,
                                       @PathParam("agent_name") String agent_name) {
    validateRegistration(registration);

    RegistrationResponse response = new RegistrationResponse();
    response.setResponseStatus(RegistrationStatus.OK);
    return response;

  }

  protected void validateRegistration(Register registration) {

  }

  @POST
  @Path("/agent/heartbeat/{agent_name}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces({MediaType.APPLICATION_JSON})
  public HeartBeatResponse heartbeat(HeartBeat message,
                                     @PathParam("agent_name") String agent_name) {
    return new HeartBeatResponse();
  }
}
