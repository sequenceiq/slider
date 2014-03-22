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
package org.apache.hoya.yarn.appmaster.web.rest.management;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hoya.api.ClusterDescription;
import org.apache.hoya.api.proto.Messages;
import org.apache.hoya.core.conf.AggregateConf;
import org.apache.hoya.yarn.appmaster.web.WebAppApi;
import org.apache.hoya.yarn.appmaster.web.rest.agent.HeartBeat;
import org.apache.hoya.yarn.appmaster.web.rest.agent.HeartBeatResponse;
import org.apache.hoya.yarn.appmaster.web.rest.agent.Register;
import org.apache.hoya.yarn.appmaster.web.rest.agent.RegistrationResponse;
import org.apache.hoya.yarn.appmaster.web.rest.agent.RegistrationStatus;
import org.apache.hoya.yarn.appmaster.web.rest.management.resources.AggregateConfResource;
import org.apache.hoya.yarn.appmaster.web.rest.management.resources.ConfTreeResource;
import org.apache.hoya.yarn.appmaster.web.rest.management.resources.ResourceFactory;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.InputStream;

/** The available REST services exposed by a slider AM. */
@Singleton
@Path(AMManagementWebServices.MANAGEMENT_CONTEXT_ROOT)
public class AMManagementWebServices {
  public static final String MANAGEMENT_CONTEXT_ROOT = "/ws/v1/slider";
  /** AM/WebApp info object */
  private WebAppApi slider;

  @Inject
  public AMManagementWebServices(WebAppApi slider) {
    this.slider = slider;
  }

  private void init(HttpServletResponse res) {
    res.setContentType(null);
  }

  @GET
  @Path("/mgmt/app")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public AggregateConfResource getAggregateConfiguration(@Context UriInfo uriInfo,
                                                         @Context HttpServletResponse res) {
    init(res);
    return ResourceFactory.createAggregateConfResource(getAggregateConf(),
                                                       uriInfo.getAbsolutePathBuilder());
  }

  @GET
  @Path("/mgmt/app/configurations/{config}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public ConfTreeResource getConfTreeResource(@PathParam("config") String config,
                                              @Context UriInfo uriInfo,
                                              @Context HttpServletResponse res) {
    init(res);
    AggregateConfResource aggregateConf =
        ResourceFactory.createAggregateConfResource(getAggregateConf(),
                                                    uriInfo.getBaseUriBuilder()
                                                        .path(
                                                            MANAGEMENT_CONTEXT_ROOT).path(
                                                        "app"));
    return aggregateConf.getConfTree(config);
  }

  protected AggregateConf getAggregateConf() {
    return slider.getAppState().getInstanceDefinition();
  }

  @POST
  @Path("/agent/register")
  @Consumes(MediaType.APPLICATION_JSON)
  public RegistrationResponse register(Register registration,
                                       @Context HttpServletResponse res) {
    init(res);
    validateRegistration(registration);

    // dummy impl
    RegistrationResponse response = new RegistrationResponse();
    response.setResponseStatus(RegistrationStatus.OK);
    return response;

  }

  protected void validateRegistration(Register registration) {

  }

  @POST
  @Path("/agent/heartbeat/{agent_name}")
  @Consumes(MediaType.APPLICATION_JSON)
  public HeartBeatResponse heartbeat(HeartBeat message,
                                     @Context HttpServletResponse res,
                                     @PathParam("agent_name") String agent_name) {
    init(res);

    // dummy impl
    return new HeartBeatResponse();
  }
}
