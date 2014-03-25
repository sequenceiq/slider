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

import org.apache.hoya.core.conf.AggregateConf;
import org.apache.hoya.yarn.appmaster.web.WebAppApi;
import org.apache.hoya.yarn.appmaster.web.rest.AMWebServices;
import org.apache.hoya.yarn.appmaster.web.rest.management.resources.AggregateConfResource;
import org.apache.hoya.yarn.appmaster.web.rest.management.resources.ConfTreeResource;
import org.apache.hoya.yarn.appmaster.web.rest.management.resources.ResourceFactory;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

/**
 *
 */
public class ManagementResource {
  private final WebAppApi slider;

  public ManagementResource(WebAppApi slider) {
    this.slider = slider;
  }

  private void init(HttpServletResponse res) {
    res.setContentType(null);
  }

  @GET
  @Path("/app")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public AggregateConfResource getAggregateConfiguration(@Context UriInfo uriInfo,
                                                         @Context HttpServletResponse res) {
    init(res);
    return ResourceFactory.createAggregateConfResource(getAggregateConf(),
                                                       uriInfo.getAbsolutePathBuilder());
  }

  @GET
  @Path("/app/configurations/{config}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public ConfTreeResource getConfTreeResource(@PathParam("config") String config,
                                              @Context UriInfo uriInfo,
                                              @Context HttpServletResponse res) {
    init(res);
    AggregateConfResource aggregateConf =
        ResourceFactory.createAggregateConfResource(getAggregateConf(),
                                                    uriInfo.getBaseUriBuilder()
                                                    .path(AMWebServices.WS_CONTEXT_ROOT).path(
                                                    "mgmt/app"));
    return aggregateConf.getConfTree(config);
  }

  protected AggregateConf getAggregateConf() {
    return slider.getAppState().getInstanceDefinitionSnapshot();
  }
}
