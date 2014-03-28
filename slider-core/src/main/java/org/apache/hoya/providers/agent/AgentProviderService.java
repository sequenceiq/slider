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
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hoya.HoyaKeys;
import org.apache.hoya.api.ClusterDescription;
import org.apache.hoya.api.OptionKeys;
import org.apache.hoya.api.StatusKeys;
import org.apache.hoya.core.conf.AggregateConf;
import org.apache.hoya.core.conf.ConfTreeOperations;
import org.apache.hoya.core.conf.MapOperations;
import org.apache.hoya.core.launch.CommandLineBuilder;
import org.apache.hoya.exceptions.BadCommandArgumentsException;
import org.apache.hoya.exceptions.BadConfigException;
import org.apache.hoya.exceptions.HoyaException;
import org.apache.hoya.providers.AbstractProviderService;
import org.apache.hoya.providers.ProviderCore;
import org.apache.hoya.providers.ProviderRole;
import org.apache.hoya.providers.ProviderUtils;
import org.apache.hoya.tools.HoyaFileSystem;
import org.apache.hoya.tools.HoyaUtils;
import org.apache.hoya.yarn.appmaster.state.StateAccessForProviders;
import org.apache.hoya.yarn.appmaster.web.rest.agent.AgentCommandType;
import org.apache.hoya.yarn.appmaster.web.rest.agent.AgentRestOperations;
import org.apache.hoya.yarn.appmaster.web.rest.agent.CommandReport;
import org.apache.hoya.yarn.appmaster.web.rest.agent.ExecutionCommand;
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
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
  public static final String INSTALL_COMMAND = "INSTALL";
  private static final String START_COMMAND = "START";
  private static int COMPONENT_NOT_INSTALLED = 0;
  private static int COMPONENT_INSTALLING = 1;
  private static int COMPONENT_INSTALLED = 2;
  private static int COMPONENT_STARTING = 3;
  private static int COMPONENT_STARTED = 4;
  private static int COMPONENT_FAILED = 100;
  private static String LABEL_MAKER = "___";
  private AgentClientProvider clientProvider;
  private HoyaFileSystem hoyaFileSystem = null;

  private Map<String, Integer> componentStatuses = new HashMap<String, Integer>();

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

    String agentImage = instanceDefinition.getInternalOperations().
        get(OptionKeys.INTERNAL_APPLICATION_IMAGE_PATH);
    LocalResource agentImageRes = hoyaFileSystem.createAmResource(new Path(agentImage), LocalResourceType.ARCHIVE);
    localResources.put(AgentKeys.AGENT_INSTALL_DIR, agentImageRes);

    String appDef = instanceDefinition.getAppConfOperations().
        getGlobalOptions().getMandatoryOption(AgentKeys.APP_DEF);
    LocalResource appDefRes = hoyaFileSystem.createAmResource(new Path(appDef),
        LocalResourceType.ARCHIVE);
    localResources.put(AgentKeys.APP_DEFINITION_DIR, appDefRes);

    String agentConf = instanceDefinition.getAppConfOperations().
        getGlobalOptions().getMandatoryOption(AgentKeys.AGENT_CONF);
    LocalResource agentConfRes = hoyaFileSystem.createAmResource(new Path(agentConf),
        LocalResourceType.FILE);
    localResources.put(AgentKeys.AGENT_CONFIG_FILE, agentConfRes);

    String agentVer = instanceDefinition.getAppConfOperations().
        getGlobalOptions().getMandatoryOption(AgentKeys.AGENT_VERSION);
    LocalResource agentVerRes = hoyaFileSystem.createAmResource(new Path(agentVer),
        LocalResourceType.FILE);
    localResources.put(AgentKeys.AGENT_VERSION_FILE, agentVerRes);

    ctx.setLocalResources(localResources);

    List<String> commandList = new ArrayList<String>();
    CommandLineBuilder operation = new CommandLineBuilder();

    operation.add("python");

    operation.add(AgentKeys.AGENT_MAIN_SCRIPT);
    operation.add(ARG_LABEL);
    operation.add(getContainerLabel(container, role));
    operation.add(ARG_HOST);
    operation.add(getClusterInfoPropertyValue(StatusKeys.INFO_AM_HOSTNAME));
    operation.add(ARG_PORT);
    operation.add(getClusterInfoPropertyValue(StatusKeys.INFO_AM_WEB_PORT));

    commandList.add(operation.build());
    ctx.setCommands(commandList);
    ctx.setEnvironment(env);
  }

  private String getContainerLabel(Container container, String role) {
    return container.getId().toString() + LABEL_MAKER + role;
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
    HeartBeatResponse response = new HeartBeatResponse();
    long id = heartBeat.getResponseId();
    response.setResponseId(id + 1L);

    String hostName = heartBeat.getHostname();
    String roleName = getRoleName(hostName);
    StateAccessForProviders accessor = getStateAccessor();
    String scriptPath = null;
    try {
      scriptPath = accessor.getClusterStatus().getMandatoryRoleOpt(roleName, "role.script");
    } catch (BadConfigException bce) {
      log.error("role.script is unavailable for " + roleName + ". Commands will not be sent.");
      return response;
    }

    if(!componentStatuses.containsKey(roleName)) {
      componentStatuses.put(roleName, 0);
    }
    int componentStatus = componentStatuses.get(roleName).intValue();

    List<CommandReport> reports = heartBeat.getReports();
    if (reports != null && reports.size() > 0) {
      for (CommandReport report : reports) {
        if (report.getStatus().equals("COMPLETED")) {
          componentStatus++;
          log.info("Component operation succeeded. Status: " + componentStatus);
        }
        if (report.getStatus().equals("FAILED")) {
          componentStatus = COMPONENT_FAILED;
          log.warn("Component instance failed operation.");
        }
      }
    }

    int waitForCount = accessor.getInstanceDefinitionSnapshot().
        getAppConfOperations().getComponentOptInt(roleName, "wait.heartbeat", 0);

    if (id < waitForCount) {
      log.info("Waiting until heartbeat count " + waitForCount + ". Current val: " + id);
      componentStatuses.put(roleName, componentStatus);
      return response;
    }

    try {
      if (componentStatus == COMPONENT_NOT_INSTALLED) {
        log.info("Installing component ...");
        addInstallCommand(roleName, response, scriptPath);
        componentStatus = COMPONENT_INSTALLING;
      } else if (componentStatus == COMPONENT_INSTALLED) {
        log.info("Starting component ...");
        addStartCommand(roleName, response, scriptPath);
        componentStatus = COMPONENT_STARTING;
      }
    } catch (HoyaException e) {
      componentStatus = COMPONENT_FAILED;
      log.warn("Component instance failed operation.", e);
    }

    componentStatuses.put(roleName, componentStatus);
    return response;
  }

  private String getRoleName(String label) {
    return label.substring(label.indexOf(LABEL_MAKER) + LABEL_MAKER.length());
  }

  private void addInstallCommand(String roleName, HeartBeatResponse response, String scriptPath) throws HoyaException{
    assert getStateAccessor().isApplicationLive();
    ConfTreeOperations appConf = getStateAccessor().getAppConfSnapshot();
    ConfTreeOperations resourcesConf = getStateAccessor().getResourcesSnapshot();
    ConfTreeOperations internalsConf = getStateAccessor().getInternalsSnapshot();

    ExecutionCommand cmd = new ExecutionCommand(AgentCommandType.EXECUTION_COMMAND);
    String clusterName = internalsConf.get(OptionKeys.APPLICATION_NAME);
    cmd.setClusterName(clusterName);
    cmd.setRoleCommand(INSTALL_COMMAND);
    cmd.setServiceName(clusterName);
    cmd.setComponentName(roleName);
    cmd.setRole(roleName);
    cmd.setTaskId(1);
    cmd.setCommandId("1-1");
    Map<String, String> hostLevelParams = new TreeMap<String, String>();
    hostLevelParams.put(JAVA_HOME, appConf.getGlobalOptions().getMandatoryOption(JAVA_HOME));
    hostLevelParams.put(PACKAGE_LIST, "[{\"type\":\"tarball\",\"name\":\"" +
                                        appConf.getGlobalOptions().getMandatoryOption(
                                            PACKAGE_LIST) +"\"}]");
    cmd.setHostLevelParams(hostLevelParams);

    setInstallCommandConfigurations(cmd);

    setCommandParameters(scriptPath, cmd);

    cmd.setHostname(getClusterInfoPropertyValue(StatusKeys.INFO_AM_HOSTNAME));
    response.addExecutionCommand(cmd);
  }

  private void setCommandParameters(String scriptPath, ExecutionCommand cmd) {
    Map<String, String> cmdParams = new TreeMap<String, String>();
    cmdParams.put("service_package_folder", "${AGENT_WORK_ROOT}/work/app/definition/package");
    cmdParams.put("script", scriptPath);
    cmdParams.put("schema_version", "2.0");
    cmdParams.put("command_timeout", "300");
    cmdParams.put("script_type", "PYTHON");
    cmd.setCommandParams(cmdParams);
  }

  private void setInstallCommandConfigurations(ExecutionCommand cmd) {
    Map<String, Map<String, String>> configurations = new TreeMap<String, Map<String, String>>();
    Map<String, String> config = new HashMap<String, String>();
    config.put("app_install_dir", "${AGENT_WORK_ROOT}/app/install");
    configurations.put("global", config);
    cmd.setConfigurations(configurations);
  }

  private void addStartCommand(String roleName, HeartBeatResponse response, String scriptPath) throws HoyaException {
    assert getStateAccessor().isApplicationLive();
    ConfTreeOperations appConf = getStateAccessor().getAppConfSnapshot();
    ConfTreeOperations resourcesConf = getStateAccessor().getResourcesSnapshot();
    ConfTreeOperations internalsConf = getStateAccessor().getInternalsSnapshot();

    ExecutionCommand cmd = new ExecutionCommand(AgentCommandType.EXECUTION_COMMAND);
    String clusterName = internalsConf.get(OptionKeys.APPLICATION_NAME);
    String hostName = getClusterInfoPropertyValue(StatusKeys.INFO_AM_HOSTNAME);
    cmd.setHostname(hostName);
    cmd.setClusterName(clusterName);
    cmd.setRoleCommand(START_COMMAND);
    cmd.setServiceName(clusterName);
    cmd.setComponentName(roleName);
    cmd.setRole(roleName);
    cmd.setTaskId(2);
    cmd.setCommandId("2-1");
    Map<String, String> hostLevelParams = new TreeMap<String, String>();
    hostLevelParams.put(JAVA_HOME, appConf.getGlobalOptions().getMandatoryOption(JAVA_HOME));
    cmd.setHostLevelParams(hostLevelParams);

    setCommandParameters(scriptPath, cmd);

    Map<String, Map<String, String>> configurations = new TreeMap<String, Map<String, String>>();

    Map<String,String> tokens = new HashMap<String, String>();
    String nnuri = appConf.get("site.fs.defaultFS");
    tokens.put("${NN_URI}", nnuri);
    tokens.put("${NN_HOST}", URI.create(nnuri).getHost());
    tokens.put("${ZK_HOST}", appConf.get("zookeeper.hosts"));

    List<String> configs = getApplicationConfigurationTypes(appConf);

    //Add global
    for (String configType : configs ) {
      addNamedConfiguration(configType, appConf.getGlobalOptions().options,
                            configurations, tokens);
    }

    cmd.setConfigurations(configurations);
    response.addExecutionCommand(cmd);
  }

  private List<String> getApplicationConfigurationTypes(ConfTreeOperations appConf) {
    // for now, reading this from appConf.  In the future, modify this method to
    // process metainfo.xml
    List<String> configList = new ArrayList<String>();
    configList.add("global");

    String configTypes = appConf.get("config_types");
    String[] configs = configTypes.split(",");

    configList.addAll(Arrays.asList(configs));

    // remove duplicates.  mostly worried about 'global' being listed
    return new ArrayList<String>(new HashSet<String>(configList));
  }

  private void addNamedConfiguration(String configName, Map<String,String> sourceConfig,
                                     Map<String, Map<String, String>> configurations,
                                     Map<String,String> tokens) {
    Map<String, String> config = new HashMap<String, String>();
    providerUtils.propagateSiteOptions(sourceConfig, config, configName, tokens);
    configurations.put(configName, config);
  }
}
