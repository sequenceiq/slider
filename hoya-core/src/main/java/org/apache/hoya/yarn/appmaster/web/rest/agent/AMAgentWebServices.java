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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/** The available REST services exposed by a slider AM. */
public class AMAgentWebServices {
  protected static final Logger log =
    LoggerFactory.getLogger(AMAgentWebServices.class);
  /** AM/WebApp info object */
  private WebAppApi slider;

  @Inject
  public AMAgentWebServices(WebAppApi slider) {
    this.slider = slider;
  }

  @GET
  @Path("/agent/register")
  public Response endpointAgentRegister() {
    Response response = Response.status(200).entity("/agent/register").build();
    return response;
  }

  @GET
  @Path("/agent")
  public Response endpointAgent() {
    Response response = Response.status(200).entity("/agent").build();
    return response;
  }
  @GET
  @Path("/")
  public Response endpointRoot() {
    Response response = Response.status(200).entity("/").build();
    return response;
  }


  @POST
  @Path("/agent/register/{agent_name: [a-zA-Z][a-zA-Z_0-9]*}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public RegistrationResponse register(Register registration,
                                       @PathParam("agent_name") String agent_name) {
    log.info("Registration of {}", agent_name);
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
