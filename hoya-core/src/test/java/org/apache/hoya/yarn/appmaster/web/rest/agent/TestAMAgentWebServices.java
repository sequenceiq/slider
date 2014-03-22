/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.WebAppDescriptor;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hoya.exceptions.BadClusterStateException;
import org.apache.hoya.exceptions.BadConfigException;
import org.apache.hoya.tools.HoyaUtils;
import org.apache.hoya.yarn.appmaster.state.AppState;
import org.apache.hoya.yarn.appmaster.web.WebAppApi;
import org.apache.hoya.yarn.appmaster.web.WebAppApiImpl;
import org.apache.hoya.yarn.appmaster.web.rest.SliderJacksonJaxbJsonProvider;
import org.apache.hoya.yarn.model.mock.MockFactory;
import org.apache.hoya.yarn.model.mock.MockHoyaClusterProtocol;
import org.apache.hoya.yarn.model.mock.MockProviderService;
import org.apache.hoya.yarn.model.mock.MockRecordFactory;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class TestAMAgentWebServices extends JerseyTest {

  public static final int RM_MAX_RAM = 4096;
  public static final int RM_MAX_CORES = 64;
  static MockFactory factory = new MockFactory();
  private static Configuration conf = new Configuration();
  private static WebAppApi slider;

  private static Injector injector = createInjector();
  private static FileSystem fs;

  public static class GuiceServletConfig extends GuiceServletContextListener {

    public GuiceServletConfig() {
      super();
    }

    @Override
    protected Injector getInjector() {
      return injector;
    }
  }

  @Path("/ws/v1/slider/agent")
  public static class MockAMAgentWebServices extends AMAgentWebServices {

    @Inject
    public MockAMAgentWebServices(WebAppApi slider) {
      super(slider);
    }

  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    injector = createInjector();
    YarnConfiguration conf = HoyaUtils.createConfiguration();
    fs = FileSystem.get(new URI("file:///"), conf);
  }

  private static Injector createInjector() {
    return Guice.createInjector(new ServletModule() {
      @Override
      protected void configureServlets() {

        AppState appState = null;
        try {
          fs = FileSystem.get(new URI("file:///"), conf);
          File
              historyWorkDir =
              new File("target/history", "TestAMAgentWebServices");
          org.apache.hadoop.fs.Path
              historyPath =
              new org.apache.hadoop.fs.Path(historyWorkDir.toURI());
          fs.delete(historyPath, true);
          appState = new AppState(new MockRecordFactory());
          appState.setContainerLimits(RM_MAX_RAM, RM_MAX_CORES);
          appState.buildInstance(
              factory.newInstanceDefinition(0, 0, 0),
              new Configuration(false),
              factory.ROLES,
              fs,
              historyPath,
              null);
        } catch (IOException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (URISyntaxException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (BadClusterStateException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (BadConfigException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        slider = new WebAppApiImpl(new MockHoyaClusterProtocol(), appState,
                                   new MockProviderService());

        bind(SliderJacksonJaxbJsonProvider.class);
        bind(MockAMAgentWebServices.class);
        bind(GenericExceptionHandler.class);
        bind(WebAppApi.class).toInstance(slider);
        bind(Configuration.class).toInstance(conf);

        Map<String, String> params = new HashMap<String, String>();
        params.put("com.sun.jersey.spi.container.ContainerRequestFilters", "com.sun.jersey.api.container.filter.LoggingFilter");
        params.put("com.sun.jersey.spi.container.ContainerResponseFilters", "com.sun.jersey.api.container.filter.LoggingFilter");
        serve("/*").with(GuiceContainer.class, params);
      }
    });
  }

  public TestAMAgentWebServices() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.appmaster.web")
              .contextListenerClass(GuiceServletConfig.class)
              .filterClass(com.google.inject.servlet.GuiceFilter.class)
              .initParam("com.sun.jersey.api.json.POJOMappingFeature", "true")
              .contextPath("hoyaam").servletPath("/").build());
  }

  @Test
  public void testRegistration() throws JSONException, Exception {
    RegistrationResponse response;
    ClientConfig clientConfig = new DefaultClientConfig();
    clientConfig.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
    Client client = Client.create(clientConfig);
    WebResource webResource = client.resource("http://localhost:9998/hoyaam/ws/v1/slider/agent/register/test");
    response = webResource.type(MediaType.APPLICATION_JSON)
        .post(RegistrationResponse.class, createDummyJSONRegister());
    Assert.assertEquals(RegistrationStatus.OK, response.getResponseStatus());
  }

  @Test
  public void testHeartbeat() throws JSONException, Exception {
    HeartBeatResponse response;
    ClientConfig clientConfig = new DefaultClientConfig();
    clientConfig.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
    Client client = Client.create(clientConfig);
    WebResource webResource = client.resource("http://localhost:9998/hoyaam/ws/v1/slider/agent/heartbeat/test");
    response = webResource.type(MediaType.APPLICATION_JSON)
        .post(HeartBeatResponse.class, createDummyHeartBeat());
    Assert.assertEquals(response.getResponseId(), 0L);
  }

  private Register createDummyJSONRegister() throws JSONException {
    Register register = new Register();
    register.setResponseId(-1);
    register.setTimestamp(System.currentTimeMillis());
    register.setHostname("dummyHost");
    return register;
  }

  private JSONObject createDummyHeartBeat() throws JSONException {
    JSONObject json = new JSONObject();
    json.put("responseId", -1);
    json.put("timestamp", System.currentTimeMillis());
    json.put("hostname", "dummyHost");
    return json;
  }

}
