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
import java.util.ArrayList;
import java.util.HashMap;
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

    if (componentStatus == COMPONENT_NOT_INSTALLED) {
      log.info("Installing component ...");
      addInstallCommand(response, scriptPath);
      componentStatus = COMPONENT_INSTALLING;
    } else if (componentStatus == COMPONENT_INSTALLED) {
      log.info("Starting component ...");
      addStartCommand(response, scriptPath);
      componentStatus = COMPONENT_STARTING;
    }

    componentStatuses.put(roleName, componentStatus);
    return response;
  }

  private String getRoleName(String label) {
    return label.substring(label.indexOf(LABEL_MAKER) + LABEL_MAKER.length());
  }

  private void addInstallCommand(HeartBeatResponse response, String scriptPath) {
    ExecutionCommand cmd = new ExecutionCommand(AgentCommandType.EXECUTION_COMMAND);
    cmd.setClusterName("cl1");
    cmd.setRoleCommand("INSTALL");
    cmd.setServiceName("HBASE");
    cmd.setComponentName("HBASE_MASTER");
    cmd.setRole("HBASE_MASTER");
    cmd.setTaskId(1);
    cmd.setCommandId("1-1");
    Map<String, String> hostLevelParams = new TreeMap<String, String>();
    hostLevelParams.put("java_home", "/usr/jdk64/jdk1.7.0_45");
    hostLevelParams.put("package_list", "[{\"type\":\"tarball\",\"name\":\"files/hbase-0.96.1-hadoop2-bin.tar.tar.tar.gz\"}]");
    cmd.setHostLevelParams(hostLevelParams);

    Map<String, Map<String, String>> configurations = new TreeMap<String, Map<String, String>>();
    Map<String, String> config = new HashMap<String, String>();
    config.put("app_install_dir", "${AGENT_WORK_ROOT}/app/install");
    configurations.put("global", config);
    cmd.setConfigurations(configurations);

    Map<String, String> cmdParams = new TreeMap<String, String>();
    cmdParams.put("service_package_folder", "${AGENT_WORK_ROOT}/work/app/definition/package");
    cmdParams.put("script", scriptPath);
    cmdParams.put("schema_version", "2.0");
    cmdParams.put("command_timeout", "300");
    cmdParams.put("script_type", "PYTHON");
    cmd.setCommandParams(cmdParams);

    cmd.setHostname(getClusterInfoPropertyValue(StatusKeys.INFO_AM_HOSTNAME));
    response.addExecutionCommand(cmd);
  }

  private void addStartCommand(HeartBeatResponse response, String scriptPath) {
    ExecutionCommand cmd = new ExecutionCommand(AgentCommandType.EXECUTION_COMMAND);
    String hostName = getClusterInfoPropertyValue(StatusKeys.INFO_AM_HOSTNAME);
    cmd.setHostname(hostName);
    cmd.setClusterName("cl1");
    cmd.setRoleCommand("START");
    cmd.setServiceName("HBASE");
    cmd.setComponentName("HBASE_MASTER");
    cmd.setRole("HBASE_MASTER");
    cmd.setTaskId(2);
    cmd.setCommandId("1-2");
    Map<String, String> hostLevelParams = new TreeMap<String, String>();
    hostLevelParams.put("java_home", "/usr/jdk64/jdk1.7.0_45");
    cmd.setHostLevelParams(hostLevelParams);

    Map<String, String> cmdParams = new TreeMap<String, String>();
    cmdParams.put("service_package_folder", "${AGENT_WORK_ROOT}/work/app/definition/package");
    cmdParams.put("script", scriptPath);
    cmdParams.put("schema_version", "2.0");
    cmdParams.put("command_timeout", "300");
    cmdParams.put("script_type", "PYTHON");
    cmd.setCommandParams(cmdParams);

    Map<String, Map<String, String>> configurations = new TreeMap<String, Map<String, String>>();
    //Add global
    Map<String, String> config = new HashMap<String, String>();
    config.put("app_user", "yarn");
    config.put("app_log_dir", "${AGENT_LOG_ROOT}/app/log");
    config.put("app_pid_dir", "${AGENT_WORK_ROOT}/app/run");
    config.put("app_root", "${AGENT_WORK_ROOT}/app/install/hbase-0.96.1-hadoop2");
    config.put("app_install_dir", "${AGENT_WORK_ROOT}/app/install");
    config.put("hbase_master_heapsize", "1024m");
    config.put("hbase_regionserver_heapsize", "1024m");
    config.put("user_group", "hadoop");
    config.put("security_enabled", "false");
    configurations.put("global", config);

    //Add hbase-site
    config = new HashMap<String, String>();
    config.put("hbase.hstore.flush.retries.number", "120");
    config.put("hbase.client.keyvalue.maxsize", "10485760");
    config.put("hbase.hstore.compactionThreshold", "3");
    config.put("hbase.rootdir", "hdfs://" + hostName + ":8020/apps/hbase/data");
    config.put("hbase.stagingdir", "hdfs://" + hostName + ":8020/apps/hbase/staging");
    config.put("hbase.regionserver.handler.count", "60");
    config.put("hbase.regionserver.global.memstore.lowerLimit", "0.38");
    config.put("hbase.hregion.memstore.block.multiplier", "2");
    config.put("hbase.hregion.memstore.flush.size", "134217728");
    config.put("hbase.superuser", "yarn");
    config.put("hbase.zookeeper.property.clientPort", "2181");
    config.put("hbase.regionserver.global.memstore.upperLimit", "0.4");
    config.put("zookeeper.session.timeout", "30000");
    config.put("hbase.tmp.dir", "${AGENT_WORK_ROOT}/work/app/tmp");
    config.put("hbase.local.dir", "${hbase.tmp.dir}/local");
    config.put("hbase.hregion.max.filesize", "10737418240");
    config.put("hfile.block.cache.size", "0.40");
    config.put("hbase.security.authentication", "simple");
    config.put("hbase.defaults.for.version.skip", "true");
    config.put("hbase.zookeeper.quorum", "" + hostName + "");
    config.put("zookeeper.znode.parent", "/hbase-unsecure");
    config.put("hbase.hstore.blockingStoreFiles", "10");
    config.put("hbase.hregion.majorcompaction", "86400000");
    config.put("hbase.security.authorization", "false");
    config.put("hbase.cluster.distributed", "true");
    config.put("hbase.hregion.memstore.mslab.enabled", "true");
    config.put("hbase.client.scanner.caching", "100");
    config.put("hbase.zookeeper.useMulti", "true");
    config.put("hbase.regionserver.info.port", "0");
    config.put("hbase.master.info.port", "60010");
    config.put("hbase.regionserver.port", "0");

    configurations.put("hbase-site", config);

    //Add core-site
    config = new HashMap<String, String>();
    config.put("fs.defaultFS", "hdfs://" + hostName + ":8020");
    configurations.put("core-site", config);

    //Add hdfs-site
    config = new HashMap<String, String>();
    config.put("dfs.namenode.https-address", hostName + ":50470");
    config.put("dfs.namenode.http-address", hostName + ":50070");
    configurations.put("hdfs-site", config);

    cmd.setConfigurations(configurations);
    response.addExecutionCommand(cmd);
  }
}
