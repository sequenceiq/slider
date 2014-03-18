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

package org.apache.hoya.providers.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hoya.HoyaKeys;
import org.apache.hoya.api.ClusterDescription;
import org.apache.hoya.api.OptionKeys;
import org.apache.hoya.api.RoleKeys;
import org.apache.hoya.api.StatusKeys;
import org.apache.hoya.core.conf.AggregateConf;
import org.apache.hoya.core.conf.MapOperations;
import org.apache.hoya.core.launch.CommandLineBuilder;
import org.apache.hoya.exceptions.BadCommandArgumentsException;
import org.apache.hoya.exceptions.HoyaException;
import org.apache.hoya.exceptions.HoyaInternalStateException;
import org.apache.hoya.providers.AbstractProviderService;
import org.apache.hoya.providers.ProviderCore;
import org.apache.hoya.providers.ProviderRole;
import org.apache.hoya.providers.ProviderUtils;
import org.apache.hoya.tools.ConfigHelper;
import org.apache.hoya.tools.HoyaFileSystem;
import org.apache.hoya.tools.HoyaUtils;
import org.apache.hoya.yarn.service.EventCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * This class implements the server-side aspects
 * of an HBase Cluster
 */
public class HBaseProviderService extends AbstractProviderService implements
                                                                  ProviderCore,
                                                                  HBaseKeys,
                                                                  HoyaKeys {

  public static final String ERROR_UNKNOWN_ROLE = "Unknown role ";
  protected static final Logger log =
    LoggerFactory.getLogger(HBaseProviderService.class);
  protected static final String NAME = "hbase";
  private static final ProviderUtils providerUtils = new ProviderUtils(log);
  private HBaseClientProvider clientProvider;
  private Configuration siteConf;
  private HoyaFileSystem hoyaFileSystem = null;

  public HBaseProviderService() {
    super("HBaseProviderService");
  }

  @Override
  public List<ProviderRole> getRoles() {
    return HBaseRoles.getRoles();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    clientProvider = new HBaseClientProvider(conf);
  }

  @Override
  public Configuration loadProviderConfigurationInformation(File confDir)
    throws BadCommandArgumentsException, IOException {

    return loadProviderConfigurationInformation(confDir, SITE_XML);
  }

  /**
   * Validate the cluster specification. This can be invoked on both
   * server and client
   * @param clusterSpec
   */
  @Override // Client and Server
  public void validateClusterSpec(ClusterDescription clusterSpec) throws
                                                                  HoyaException {
    clientProvider.validateClusterSpec(clusterSpec);
  }


  /**
   * Get the path to hbase home
   * @return the hbase home path
   */
  public String buildHBaseScriptBinPath(ClusterDescription cd) throws
                                                               FileNotFoundException {
    return providerUtils.buildPathToScript(cd, "bin",
                                           HBaseKeys.HBASE_SCRIPT);
  }

  @Override
  public void buildContainerLaunchContext(ContainerLaunchContext ctx,
                                          Container container,
                                          String role,
                                          HoyaFileSystem hoyaFileSystem,
                                          Path generatedConfPath,
                                          MapOperations roleOptions,
                                          Path containerTmpDirPath,
                                          AggregateConf instanceDefinition) throws
                                                                            IOException,
                                                                            HoyaException {

    this.hoyaFileSystem = hoyaFileSystem;
    this.instanceDefinition = instanceDefinition;
    // Set the environment
    Map<String, String> env = HoyaUtils.buildEnvMap(roleOptions);

    env.put(HBASE_LOG_DIR, providerUtils.getLogdir());

    env.put("PROPAGATED_CONFDIR", ApplicationConstants.Environment.PWD.$()+"/"+
                                  HoyaKeys.PROPAGATED_CONF_DIR_NAME);


    //local resources
    Map<String, LocalResource> localResources =
      new HashMap<String, LocalResource>();

    //add the configuration resources
    Map<String, LocalResource> confResources;
    confResources = hoyaFileSystem.submitDirectory(
            generatedConfPath,
            HoyaKeys.PROPAGATED_CONF_DIR_NAME);
    localResources.putAll(confResources);
    //Add binaries
    //now add the image if it was set
    String imageURI = instanceDefinition.getInternalOperations().get(OptionKeys.APPLICATION_IMAGE_PATH);
      hoyaFileSystem.maybeAddImagePath(localResources, imageURI);
    ctx.setLocalResources(localResources);
    List<String> commands = new ArrayList<String>();

    CommandLineBuilder command = new CommandLineBuilder();

    String heap = roleOptions.getOption(RoleKeys.JVM_HEAP, DEFAULT_JVM_HEAP);
    if (HoyaUtils.isSet(heap)) {
      String adjustedHeap = HoyaUtils.translateTrailingHeapUnit(heap);
      env.put("HBASE_HEAPSIZE", adjustedHeap);
    }
    
    String gcOpts = roleOptions.getOption(RoleKeys.GC_OPTS, DEFAULT_GC_OPTS);
    if (HoyaUtils.isSet(gcOpts)) {
      env.put("SERVER_GC_OPTS", gcOpts);
    }
    
    //this must stay relative if it is an image
    command.add(providerUtils.buildPathToScript(
      instanceDefinition.getInternalOperations(),
      "bin",
      HBaseKeys.HBASE_SCRIPT));
    //config dir is relative to the generated file
    command.add(ARG_CONFIG);
    command.add("$PROPAGATED_CONFDIR");

    String roleCommand;
    String logfile;
    //now look at the role
    if (ROLE_WORKER.equals(role)) {
      //role is region server
      roleCommand = REGION_SERVER;
      logfile = "/region-server.txt";
    } else if (ROLE_MASTER.equals(role)) {
      roleCommand = MASTER;
      
      logfile ="/master.txt";
    } else {
      throw new HoyaInternalStateException("Cannot start role %s", role);
    }

    command.add(roleCommand);
    command.add(ACTION_START);
    //log details
    command.addOutAndErrFiles(logfile, null);

    String cmdStr = command.build();


    commands.add(cmdStr);
    ctx.setCommands(commands);
    ctx.setEnvironment(env);

  }

  /**
   * Run this service
   *
   *
   * @param cd component description
   * @param confDir local dir with the config
   * @param env environment variables above those generated by
   * @param execInProgress callback for the event notification
   * @throws IOException IO problems
   * @throws HoyaException anything internal
   */
  @Override
  public boolean exec(ClusterDescription cd,
                      File confDir,
                      Map<String, String> env,
                      EventCallback execInProgress) throws
                                                 IOException,
                                                 HoyaException {

    return false;
  }


  /**
   * This is a validation of the application configuration on the AM.
   * Here is where things like the existence of keytabs and other
   * not-seen-client-side properties can be tested, before
   * the actual process is spawned. 
   * @param clusterSpec clusterSpecification
   * @param confDir configuration directory
   * @param secure flag to indicate that secure mode checks must exist
   * @throws IOException IO problemsn
   * @throws HoyaException any failure
   */
  @Override
  public void validateApplicationConfiguration(ClusterDescription clusterSpec,
                                               File confDir,
                                               boolean secure
                                              ) throws IOException, HoyaException {
    String siteXMLFilename = SITE_XML;
    File siteXML = new File(confDir, siteXMLFilename);
    if (!siteXML.exists()) {
      throw new BadCommandArgumentsException(
        "Configuration directory %s doesn't contain %s - listing is %s",
        confDir, siteXMLFilename, HoyaUtils.listDir(confDir));
    }

    //now read it in
    siteConf = ConfigHelper.loadConfFromFile(siteXML);
    //look in the site spec to see that it is OK
    clientProvider.validateHBaseSiteXML(siteConf, secure, siteXMLFilename);
    
    if (secure) {
      //secure mode: take a look at the keytab of master and RS
      HoyaUtils.verifyKeytabExists(siteConf,
                                   HBaseConfigFileOptions.KEY_MASTER_KERBEROS_KEYTAB);
      HoyaUtils.verifyKeytabExists(siteConf,
                                   HBaseConfigFileOptions.KEY_REGIONSERVER_KERBEROS_KEYTAB);

    }
  }


  /**
   * Build the provider status, can be empty
   * @return the provider status - map of entries to add to the info section
   */
  public Map<String, String> buildProviderStatus() {
    Map<String, String> stats = new HashMap<String, String>();

    return stats;
  }
  
  /* non-javadoc
   * @see org.apache.hoya.providers.ProviderService#buildMonitorDetails()
   */
  @Override
  public TreeMap<String,URL> buildMonitorDetails(ClusterDescription clusterDesc) {
    TreeMap<String,URL> map = new TreeMap<String,URL>();
    
    map.put("Active HBase Master (RPC): " + getInfoAvoidingNull(clusterDesc, StatusKeys.INFO_MASTER_ADDRESS), null);

    return map;
  }
}
