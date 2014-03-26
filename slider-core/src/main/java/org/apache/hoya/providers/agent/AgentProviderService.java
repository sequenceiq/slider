/*
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

package org.apache.hoya.providers.agent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hoya.HoyaKeys;
import org.apache.hoya.api.ClusterDescription;
import org.apache.hoya.api.OptionKeys;
import org.apache.hoya.api.StatusKeys;
import org.apache.hoya.core.conf.AggregateConf;
import org.apache.hoya.core.conf.ConfTreeOperations;
import org.apache.hoya.core.conf.MapOperations;
import org.apache.hoya.core.launch.CommandLineBuilder;
import org.apache.hoya.exceptions.BadCommandArgumentsException;
import org.apache.hoya.exceptions.HoyaException;
import org.apache.hoya.providers.AbstractProviderService;
import org.apache.hoya.providers.ProviderCore;
import org.apache.hoya.providers.ProviderRole;
import org.apache.hoya.providers.ProviderUtils;
import org.apache.hoya.tools.HoyaFileSystem;
import org.apache.hoya.tools.HoyaUtils;
import org.apache.hoya.yarn.appmaster.state.StateAccessForProviders;
import org.apache.hoya.yarn.appmaster.web.rest.agent.AgentRestOperations;
import org.apache.hoya.yarn.appmaster.web.rest.agent.HeartBeat;
import org.apache.hoya.yarn.appmaster.web.rest.agent.HeartBeatResponse;
import org.apache.hoya.yarn.appmaster.web.rest.agent.Register;
import org.apache.hoya.yarn.appmaster.web.rest.agent.RegistrationResponse;
import org.apache.hoya.yarn.appmaster.web.rest.agent.RegistrationStatus;
import org.apache.hoya.yarn.service.EventCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class implements the server-side aspects
 * of an agent deployment
 */
public class AgentProviderService extends AbstractProviderService implements
                                                                  ProviderCore,
                                                                  AgentKeys,
                                                                  HoyaKeys, AgentRestOperations {


  protected static final Logger log =
      LoggerFactory.getLogger(AgentProviderService.class);
  protected static final String NAME = "agent";
  private static final ProviderUtils providerUtils = new ProviderUtils(log);
  private AgentClientProvider clientProvider;
  private HoyaFileSystem hoyaFileSystem = null;

  public AgentProviderService() {
    super("AgentProviderService");
    setAgentRestOperations(this);
  }

  @Override
  public List<ProviderRole> getRoles() {
    return AgentRoles.getRoles();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    clientProvider = new AgentClientProvider(conf);
  }

  @Override
  public Configuration loadProviderConfigurationInformation(File confDir) throws
      BadCommandArgumentsException,
      IOException {
    return new Configuration(false);
  }

  @Override
  public void validateInstanceDefinition(AggregateConf instanceDefinition)
      throws
      HoyaException {
    clientProvider.validateInstanceDefinition(instanceDefinition);
  }

  @Override
  public void buildContainerLaunchContext(ContainerLaunchContext ctx,
                                          AggregateConf instanceDefinition,
                                          Container container,
                                          String role,
                                          HoyaFileSystem hoyaFileSystem,
                                          Path generatedConfPath,
                                          MapOperations resourceComponent,
                                          MapOperations appComponent,
                                          Path containerTmpDirPath) throws
                                                                            IOException,
                                                                            HoyaException {

    this.hoyaFileSystem = hoyaFileSystem;
    this.instanceDefinition = instanceDefinition;
    log.info("Build launch context for Agent");
    log.debug(instanceDefinition.toString());

    // Set the environment
    Map<String, String> env = HoyaUtils.buildEnvMap(appComponent);

    HoyaUtils.copyDirectory(getConf(), generatedConfPath, containerTmpDirPath,
                            null);
    Path targetConfDir = containerTmpDirPath;


    String workDir = ApplicationConstants.Environment.PWD.$();
    env.put("AGENT_WORK_ROOT", workDir);
    log.info("AGENT_WORK_ROOT set to " + workDir);
    String logDir = ApplicationConstants.Environment.LOG_DIRS.$();
    env.put("AGENT_LOG_ROOT", logDir);
    log.info("AGENT_LOG_ROOT set to " + logDir);

    //local resources
    Map<String, LocalResource> localResources =
        new HashMap<String, LocalResource>();

    //add the configuration resources
    Map<String, LocalResource> confResources;
    confResources = hoyaFileSystem.submitDirectory(
        targetConfDir,
        HoyaKeys.PROPAGATED_CONF_DIR_NAME);
    localResources.putAll(confResources);

    //now add the image if it was set
    String
        imageURI = instanceDefinition.getInternalOperations().
            get(OptionKeys.INTERNAL_APPLICATION_IMAGE_PATH);
    hoyaFileSystem.maybeAddImagePath(localResources, imageURI);
    log.info("Number of local resources" + localResources.size());
    for(String key : localResources.keySet()) {
      log.info("Resource key: " + key + " value: " + localResources.get(key));
    }
    ctx.setLocalResources(localResources);

    List<String> commandList = new ArrayList<String>();
    CommandLineBuilder operation = new CommandLineBuilder();

    operation.add("python");

    operation.add(AgentKeys.AGENT_MAIN_SCRIPT);
    operation.add(ARG_LABEL);
    operation.add(
        getClusterInfoPropertyValue(StatusKeys.INFO_AM_APP_ID) + "." +
            getClusterInfoPropertyValue(StatusKeys.INFO_AM_CONTAINER_ID));
    operation.add(ARG_HOST);
    operation.add(getClusterInfoPropertyValue(StatusKeys.INFO_AM_HOSTNAME));
    operation.add(ARG_PORT);
    operation.add(getClusterInfoPropertyValue(StatusKeys.INFO_AM_WEB_PORT));

    commandList.add(operation.build());
    ctx.setCommands(commandList);
    ctx.setEnvironment(env);
  }

  protected String getClusterInfoPropertyValue(String name) {
    StateAccessForProviders accessor = getStateAccessor();
    assert accessor.isApplicationLive();
    ClusterDescription description = accessor.getClusterStatus();
    return description.getInfo(name);
  }

  /**
   * Run this service
   *
   * @param instanceDefinition component description
   * @param confDir            local dir with the config
   * @param env                environment variables above those generated by
   * @param execInProgress     callback for the event notification
   *
   * @throws IOException   IO problems
   * @throws HoyaException anything internal
   */
  @Override
  public boolean exec(AggregateConf instanceDefinition,
                      File confDir,
                      Map<String, String> env,
                      EventCallback execInProgress) throws
      IOException,
      HoyaException {

    return false;
  }

  /**
   * Build the provider status, can be empty
   *
   * @return the provider status - map of entries to add to the info section
   */
  public Map<String, String> buildProviderStatus() {
    Map<String, String> stats = new HashMap<String, String>();
    return stats;
  }

  @Override
  public boolean isSupportedRole(String role) {
    return true;
  }

  @Override
  public RegistrationResponse handleRegistration(Register registration) {
    // dummy impl
    RegistrationResponse response = new RegistrationResponse();
    response.setResponseStatus(RegistrationStatus.OK);
    return response;
  }

  @Override
  public HeartBeatResponse handleHeartBeat(HeartBeat heartBeat) {
    // dummy impl
    long id = heartBeat.getResponseId();
    HeartBeatResponse response = new HeartBeatResponse();
    response.setResponseId(id + 1L);
    return response;
  }
}
