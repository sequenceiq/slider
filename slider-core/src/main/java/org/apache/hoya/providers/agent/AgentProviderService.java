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
import java.util.concurrent.atomic.AtomicInteger;

/** This class implements the server-side aspects of an agent deployment */
public class AgentProviderService extends AbstractProviderService implements
    ProviderCore,
    AgentKeys,
    HoyaKeys, AgentRestOperations {


  protected static final Logger log =
      LoggerFactory.getLogger(AgentProviderService.class);
  private static final ProviderUtils providerUtils = new ProviderUtils(log);
  private static String LABEL_MAKER = "___";
  private AgentClientProvider clientProvider;
  private Map<String, ComponentInstanceState> componentStatuses = new HashMap<String, ComponentInstanceState>();
  private AtomicInteger taskId = new AtomicInteger(0);

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

    this.instanceDefinition = instanceDefinition;
    log.info("Build launch context for Agent");
    log.debug(instanceDefinition.toString());

    // Set the environment
    Map<String, String> env = HoyaUtils.buildEnvMap(appComponent);

    String workDir = ApplicationConstants.Environment.PWD.$();
    env.put("AGENT_WORK_ROOT", workDir);
    log.info("AGENT_WORK_ROOT set to " + workDir);
    String logDir = ApplicationConstants.Environment.LOG_DIRS.$();
    env.put("AGENT_LOG_ROOT", logDir);
    log.info("AGENT_LOG_ROOT set to " + logDir);

    //local resources
    Map<String, LocalResource> localResources =
        new HashMap<String, LocalResource>();

    // TODO: Should agent need to support App Home
    String scriptPath = new File(AgentKeys.AGENT_MAIN_SCRIPT_ROOT, AgentKeys.AGENT_MAIN_SCRIPT).getPath();
    String appHome = instanceDefinition.getAppConfOperations().
        getGlobalOptions().get(AgentKeys.PACKAGE_PATH);
    if (appHome != null && !appHome.equals("")) {
      scriptPath = new File(appHome, AgentKeys.AGENT_MAIN_SCRIPT).getPath();
    }

    String agentImage = instanceDefinition.getInternalOperations().
        get(OptionKeys.INTERNAL_APPLICATION_IMAGE_PATH);
    if (agentImage != null) {
      LocalResource agentImageRes = hoyaFileSystem.createAmResource(new Path(agentImage), LocalResourceType.ARCHIVE);
      localResources.put(AgentKeys.AGENT_INSTALL_DIR, agentImageRes);
    }

    log.info("Using " + scriptPath + " for agent.");
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
    String label = getContainerLabel(container, role);
    CommandLineBuilder operation = new CommandLineBuilder();

    operation.add(AgentKeys.PYTHON_EXE);

    operation.add(scriptPath);
    operation.add(ARG_LABEL);
    operation.add(label);
    operation.add(ARG_HOST);
    operation.add(getClusterInfoPropertyValue(StatusKeys.INFO_AM_HOSTNAME));
    operation.add(ARG_PORT);
    operation.add(getClusterInfoPropertyValue(StatusKeys.INFO_AM_WEB_PORT));

    commandList.add(operation.build());
    ctx.setCommands(commandList);
    ctx.setEnvironment(env);

    // initialize the component instance state
    componentStatuses.put(label,
                          new ComponentInstanceState(
                              role,
                              container.getId().toString(),
                              getClusterInfoPropertyValue(OptionKeys.APPLICATION_NAME)));
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
    String label = registration.getHostname();
    if (componentStatuses.containsKey(label)) {
      response.setResponseStatus(RegistrationStatus.OK);
    } else {
      response.setResponseStatus(RegistrationStatus.FAILED);
      response.setLog("Label not recognized.");
    }
    return response;
  }

  private Command getCommand(String commandVal) {
    if (commandVal.equals(Command.START.toString())) {
      return Command.START;
    }
    if (commandVal.equals(Command.INSTALL.toString())) {
      return Command.INSTALL;
    }

    return Command.NOP;
  }

  private CommandResult getCommandResult(String commandResVal) {
    if (commandResVal.equals(CommandResult.COMPLETED.toString())) {
      return CommandResult.COMPLETED;
    }
    if (commandResVal.equals(CommandResult.FAILED.toString())) {
      return CommandResult.FAILED;
    }
    if (commandResVal.equals(CommandResult.IN_PROGRESS.toString())) {
      return CommandResult.IN_PROGRESS;
    }

    throw new IllegalArgumentException("Unrecognized value " + commandResVal);
  }

  @Override
  public HeartBeatResponse handleHeartBeat(HeartBeat heartBeat) {
    // dummy impl
    HeartBeatResponse response = new HeartBeatResponse();
    long id = heartBeat.getResponseId();
    response.setResponseId(id + 1L);

    String label = heartBeat.getHostname();
    String roleName = getRoleName(label);
    StateAccessForProviders accessor = getStateAccessor();
    String scriptPath;
    try {
      scriptPath = accessor.getClusterStatus().getMandatoryRoleOpt(roleName, AgentKeys.COMPONENT_SCRIPT);
    } catch (BadConfigException bce) {
      log.error("role.script is unavailable for " + roleName + ". Commands will not be sent.");
      return response;
    }

    if (!componentStatuses.containsKey(label)) {
      return response;
    }
    ComponentInstanceState componentStatus = componentStatuses.get(label);

    List<CommandReport> reports = heartBeat.getReports();
    if (reports != null && reports.size() > 0) {
      CommandReport report = reports.get(0);
      CommandResult result = getCommandResult(report.getStatus());
      Command command = getCommand(report.getRoleCommand());
      componentStatus.applyCommandResult(result, command);
      log.info("Component operation. Status: " + result);
    }

    int waitForCount = accessor.getInstanceDefinitionSnapshot().
        getAppConfOperations().getComponentOptInt(roleName, AgentKeys.WAIT_HEARTBEAT, 0);

    if (id < waitForCount) {
      log.info("Waiting until heartbeat count " + waitForCount + ". Current val: " + id);
      componentStatuses.put(roleName, componentStatus);
      return response;
    }

    Command command = componentStatus.getNextCommand();
    if (Command.NOP != command) {
      try {
        componentStatus.commandIssued(command);
        if (command == Command.INSTALL) {
          log.info("Installing component ...");
          addInstallCommand(roleName, response, scriptPath);
        } else if (command == Command.START) {
          log.info("Starting component ...");
          addStartCommand(roleName, response, scriptPath);
        }
      } catch (HoyaException e) {
        componentStatus.applyCommandResult(CommandResult.FAILED, command);
        log.warn("Component instance failed operation.", e);
      }
    }

    return response;
  }

  private String getRoleName(String label) {
    return label.substring(label.indexOf(LABEL_MAKER) + LABEL_MAKER.length());
  }

  protected void addInstallCommand(String roleName, HeartBeatResponse response, String scriptPath)
      throws HoyaException {
    assert getStateAccessor().isApplicationLive();
    ConfTreeOperations appConf = getStateAccessor().getAppConfSnapshot();
    ConfTreeOperations resourcesConf = getStateAccessor().getResourcesSnapshot();
    ConfTreeOperations internalsConf = getStateAccessor().getInternalsSnapshot();

    ExecutionCommand cmd = new ExecutionCommand(AgentCommandType.EXECUTION_COMMAND);
    prepareExecutionCommand(cmd);
    String clusterName = internalsConf.get(OptionKeys.APPLICATION_NAME);
    cmd.setClusterName(clusterName);
    cmd.setRoleCommand(Command.INSTALL.toString());
    cmd.setServiceName(clusterName);
    cmd.setComponentName(roleName);
    cmd.setRole(roleName);
    Map<String, String> hostLevelParams = new TreeMap<String, String>();
    hostLevelParams.put(JAVA_HOME, appConf.getGlobalOptions().getMandatoryOption(JAVA_HOME));
    hostLevelParams.put(PACKAGE_LIST, "[{\"type\":\"tarball\",\"name\":\"" +
                                      appConf.getGlobalOptions().getMandatoryOption(
                                          PACKAGE_LIST) + "\"}]");
    cmd.setHostLevelParams(hostLevelParams);

    setInstallCommandConfigurations(cmd);

    setCommandParameters(scriptPath, cmd);

    cmd.setHostname(getClusterInfoPropertyValue(StatusKeys.INFO_AM_HOSTNAME));
    response.addExecutionCommand(cmd);
  }

  private void prepareExecutionCommand(ExecutionCommand cmd) {
    cmd.setTaskId(taskId.incrementAndGet());
    cmd.setCommandId(cmd.getTaskId() + "-1");
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
    addDefaultGlobalConfig(config);
    config.put("app_install_dir", "${AGENT_WORK_ROOT}/app/install");
    configurations.put("global", config);
    cmd.setConfigurations(configurations);
  }

  protected void addStartCommand(String roleName, HeartBeatResponse response, String scriptPath) throws HoyaException {
    assert getStateAccessor().isApplicationLive();
    ConfTreeOperations appConf = getStateAccessor().getAppConfSnapshot();
    ConfTreeOperations resourcesConf = getStateAccessor().getResourcesSnapshot();
    ConfTreeOperations internalsConf = getStateAccessor().getInternalsSnapshot();

    ExecutionCommand cmd = new ExecutionCommand(AgentCommandType.EXECUTION_COMMAND);
    prepareExecutionCommand(cmd);
    String clusterName = internalsConf.get(OptionKeys.APPLICATION_NAME);
    String hostName = getClusterInfoPropertyValue(StatusKeys.INFO_AM_HOSTNAME);
    cmd.setHostname(hostName);
    cmd.setClusterName(clusterName);
    cmd.setRoleCommand(Command.START.toString());
    cmd.setServiceName(clusterName);
    cmd.setComponentName(roleName);
    cmd.setRole(roleName);
    Map<String, String> hostLevelParams = new TreeMap<String, String>();
    hostLevelParams.put(JAVA_HOME, appConf.getGlobalOptions().getMandatoryOption(JAVA_HOME));
    cmd.setHostLevelParams(hostLevelParams);

    setCommandParameters(scriptPath, cmd);

    Map<String, Map<String, String>> configurations = new TreeMap<String, Map<String, String>>();

    Map<String, String> tokens = new HashMap<String, String>();
    String nnuri = appConf.get("site.fs.defaultFS");
    tokens.put("${NN_URI}", nnuri);
    tokens.put("${NN_HOST}", URI.create(nnuri).getHost());
    tokens.put("${ZK_HOST}", appConf.get("zookeeper.hosts"));

    List<String> configs = getApplicationConfigurationTypes(appConf);

    //Add global
    for (String configType : configs) {
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

  private void addNamedConfiguration(String configName, Map<String, String> sourceConfig,
                                     Map<String, Map<String, String>> configurations,
                                     Map<String, String> tokens) {
    Map<String, String> config = new HashMap<String, String>();
    if (configName.equals("global")) {
      addDefaultGlobalConfig(config);
    }
    providerUtils.propagateSiteOptions(sourceConfig, config, configName, tokens);
    configurations.put(configName, config);
  }

  private void addDefaultGlobalConfig(Map<String, String> config) {
    config.put("app_log_dir", "${AGENT_LOG_ROOT}/app/log");
    config.put("app_pid_dir", "${AGENT_WORK_ROOT}/app/run");
    config.put("app_install_dir", "${AGENT_WORK_ROOT}/app/install");
  }
}
