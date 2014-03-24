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

package org.apache.hoya.yarn.appmaster.web.rest.management;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.WebAppDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hoya.core.conf.AggregateConf;
import org.apache.hoya.core.conf.ConfTree;
import org.apache.hoya.core.persist.JsonSerDeser;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class TestAMManagementWebServices extends JerseyTest {

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

  @Path("/ws/v1/slider/mgmt")
  public static class MockSliderAMWebServices extends AMManagementWebServices {

    @Inject
    public MockSliderAMWebServices(WebAppApi slider) {
      super(slider);
    }

    protected AggregateConf getAggregateConf() {
      JsonSerDeser<ConfTree> confTreeJsonSerDeser =
          new JsonSerDeser<ConfTree>(ConfTree.class);
      ConfTree internal = null;
      ConfTree app_conf = null;
      ConfTree resources = null;
      try {
        internal =
            confTreeJsonSerDeser.fromResource(
                "/org/apache/hoya/core/conf/examples/internal.json");
        app_conf =
            confTreeJsonSerDeser.fromResource(
                "/org/apache/hoya/core/conf/examples/app_configuration.json");
        resources =
            confTreeJsonSerDeser.fromResource(
                "/org/apache/hoya/core/conf/examples/resources.json");
      } catch (IOException e) {
        fail(e.getMessage());
      }
      AggregateConf aggregateConf = new AggregateConf(
          resources,
          app_conf,
          internal);
      aggregateConf.setName("test");
      return aggregateConf;
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
              new File("target/history", "TestAMManagementWebServices");
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
              null, null);
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
        bind(MockSliderAMWebServices.class);
        bind(GenericExceptionHandler.class);
        bind(WebAppApi.class).toInstance(slider);
        bind(Configuration.class).toInstance(conf);

        serve("/*").with(GuiceContainer.class);
      }
    });
  }

  public TestAMManagementWebServices() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.appmaster.web")
              .contextListenerClass(GuiceServletConfig.class)
              .filterClass(com.google.inject.servlet.GuiceFilter.class)
              .contextPath("hoyaam").servletPath("/").build());
  }

  @Test
  public void testAppResource() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("slider").path("mgmt").path("app")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(200, response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 4, json.length());
    assertEquals("wrong href",
                 "http://localhost:9998/hoyaam/ws/v1/slider/mgmt/app",
                 json.getString("href"));
    assertNotNull("no resources", json.getJSONObject("resources"));
    assertNotNull("no internal", json.getJSONObject("internal"));
    assertNotNull("no appConf", json.getJSONObject("appConf"));
  }

  @Test
  public void testAppInternal() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse
        response =
        r.path("ws").path("v1").path("slider").path("mgmt").path("app").path("configurations").path(
            "internal")
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(200, response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 4, json.length());
    assertEquals("wrong href",
                 "http://localhost:9998/hoyaam/ws/v1/slider/mgmt/app/configurations/internal",
                 json.getString("href"));
    assertEquals("wrong description",
                 "Internal configuration DO NOT EDIT",
                 json.getJSONObject("metadata").getString("description"));
  }

  @Test
  public void testAppResources() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse
        response =
        r.path("ws").path("v1").path("slider").path("mgmt").path("app").path("configurations").path(
            "resources")
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

    assertEquals(200, response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 4, json.length());
    assertEquals("wrong href",
                 "http://localhost:9998/hoyaam/ws/v1/slider/mgmt/app/configurations/resources",
                 json.getString("href"));
    json = json.getJSONObject("components");
    assertNotNull("no components", json);
    assertEquals("incorrect number of components", 2, json.length());
    assertNotNull("wrong component", json.getJSONObject("worker"));
  }

  @Test
  public void testAppAppConf() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse
        response =
        r.path("ws").path("v1").path("slider").path("mgmt").path("app").path("configurations").path(
            "appConf")
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(200, response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 4, json.length());
    assertEquals("wrong href",
                 "http://localhost:9998/hoyaam/ws/v1/slider/mgmt/app/configurations/appConf",
                 json.getString("href"));
    json = json.getJSONObject("components");
    assertNotNull("no components", json);
    assertEquals("incorrect number of components", 2, json.length());
    assertNotNull("wrong component", json.getJSONObject("worker"));
  }
}
