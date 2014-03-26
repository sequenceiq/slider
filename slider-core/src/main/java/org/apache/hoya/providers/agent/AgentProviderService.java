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
import org.apache.hoya.api.OptionKeys;
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
  public void validateInstanceDefinition(AggregateConf instanceDefinition) throws
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
    //TODO: PATCH THE CONFIG FOR THE TARGET


    String propagatedConfDir = ApplicationConstants.Environment.PWD.$() + "/" +
        HoyaKeys.PROPAGATED_CONF_DIR_NAME;
    env.put("PROPAGATED_CONFDIR", propagatedConfDir);
    //local resources
    Map<String, LocalResource> localResources =
        new HashMap<String, LocalResource>();

    //add the configuration resources
    Map<String, LocalResource> confResources;
    confResources = hoyaFileSystem.submitDirectory(
        targetConfDir,
        HoyaKeys.PROPAGATED_CONF_DIR_NAME);
    localResources.putAll(confResources);
    //Add binaries
    //now add the image if it was set

    //Add binaries
    //now add the image if it was set
    String imageURI = instanceDefinition.getInternalOperations()
                                        .get(OptionKeys.INTERNAL_APPLICATION_IMAGE_PATH);
    hoyaFileSystem.maybeAddImagePath(localResources, imageURI);
    
    ctx.setLocalResources(localResources);
    List<String> commandList = new ArrayList<String>();
    CommandLineBuilder operation = new CommandLineBuilder();

    ConfTreeOperations appConf =
      instanceDefinition.getAppConfOperations();

    String script = appComponent.getMandatoryOption(SCRIPT_PATH);
    String packagePath = appComponent.getMandatoryOption(PACKAGE_PATH);
    File packagePathFile = new File(packagePath);
    HoyaUtils.verifyIsDir(packagePathFile, log);
    File executable = new File(packagePathFile, script);
    HoyaUtils.verifyFileExists(executable, log);

    String appHome = appComponent.getMandatoryOption(APP_HOME);
    //APP_HOME == /dev/null is being used to issue direct start commands
    //This is not required once embedded Agent is available
    if (appHome.equals("/dev/null")) {
      operation.add("python");
      operation.add(executable.getCanonicalPath());
      operation.add("START");
      operation.add(propagatedConfDir + "/" + AgentKeys.COMMAND_JSON_FILENAME);
      operation.add(packagePathFile.getCanonicalPath());
      operation.add("/tmp/strout.txt");
    } else {
      //this must stay relative if it is an image
      operation.add("python");
      operation.add(executable.getCanonicalPath());
      operation.add(ARG_LOG);
      operation.add(ApplicationConstants.LOG_DIR_EXPANSION_VAR);
      operation.add(ARG_COMMAND);
      operation.add(propagatedConfDir + "/" + AgentKeys.COMMAND_JSON_FILENAME);

      operation.add(ARG_CONFIG);
      operation.add("$PROPAGATED_CONFDIR");
    }

    String filename = "agent-server.txt";

    operation.addOutAndErrFiles(filename, null);


    commandList.add(operation.build());
    ctx.setCommands(commandList);
    ctx.setEnvironment(env);
}


  /**
   * Run this service
   *
   * @param instanceDefinition             component description
   * @param confDir        local dir with the config
   * @param env            environment variables above those generated by
   * @param execInProgress callback for the event notification
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
