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

package org.apache.hoya.providers.hoyaam;

import com.beust.jcommander.JCommander;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hoya.HoyaKeys;
import org.apache.hoya.api.OptionKeys;
import org.apache.hoya.api.ResourceKeys;
import org.apache.hoya.api.RoleKeys;
import org.apache.hoya.core.conf.AggregateConf;
import org.apache.hoya.core.conf.MapOperations;
import org.apache.hoya.core.launch.AbstractLauncher;
import org.apache.hoya.core.launch.CommandLineBuilder;
import org.apache.hoya.exceptions.BadConfigException;
import org.apache.hoya.exceptions.HoyaException;
import org.apache.hoya.providers.AbstractClientProvider;
import org.apache.hoya.providers.PlacementPolicy;
import org.apache.hoya.providers.ProviderRole;
import org.apache.hoya.providers.ProviderUtils;
import org.apache.hoya.tools.ConfigHelper;
import org.apache.hoya.tools.HoyaFileSystem;
import org.apache.hoya.tools.HoyaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * handles the setup of the Hoya AM.
 * This keeps aspects of role, cluster validation and Clusterspec setup
 * out of the core hoya client
 */
public class HoyaAMClientProvider extends AbstractClientProvider implements
                                                     HoyaKeys {


  protected static final Logger log =
    LoggerFactory.getLogger(HoyaAMClientProvider.class);
  protected static final String NAME = "hoyaAM";
  private static final ProviderUtils providerUtils = new ProviderUtils(log);
  public static final String INSTANCE_RESOURCE_BASE = PROVIDER_RESOURCE_BASE_ROOT +
                                                       "hoyaam/instance/";
  public static final String INTERNAL_JSON =
    INSTANCE_RESOURCE_BASE + "internal.json";
  public static final String APPCONF_JSON =
    INSTANCE_RESOURCE_BASE + "appconf.json";
  public static final String RESOURCES_JSON =
    INSTANCE_RESOURCE_BASE + "resources.json";

  public static final String AM_ROLE_CONFIG_RESOURCE =
    PROVIDER_RESOURCE_BASE +"hoyaam/role-am.xml";

  public HoyaAMClientProvider(Configuration conf) {
    super(conf);
  }

  /**
   * List of roles
   */
  public static final List<ProviderRole> ROLES =
    new ArrayList<ProviderRole>();

  public static final int KEY_AM = ROLE_HOYA_AM_PRIORITY_INDEX;

  /**
   * Initialize role list
   */
  static {
    ROLES.add(new ProviderRole(COMPONENT_AM, KEY_AM,
                               PlacementPolicy.EXCLUDE_FROM_FLEXING));
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public List<ProviderRole> getRoles() {
    return ROLES;
  }


  /**
   * Get a map of all the default options for the cluster; values
   * that can be overridden by user defaults after
   * @return a possibly empty map of default cluster options.
   */
  @Override
  public Configuration getDefaultClusterConfiguration() throws
                                                        FileNotFoundException {
    return ConfigHelper.loadMandatoryResource(
      "org/apache/hoya/providers/hoyaam/cluster.xml");
  }

  /**
   * Create the default cluster role instance for a named
   * cluster role; 
   *
   * @param rolename role name
   * @return a node that can be added to the JSON
   */
  @Override
  public Map<String, String> createDefaultClusterRole(String rolename) throws
                                                                       HoyaException,
                                                                       FileNotFoundException {
    Map<String, String> rolemap = new HashMap<String, String>();
    if (rolename.equals(COMPONENT_AM)) {
      Configuration conf = ConfigHelper.loadMandatoryResource(
        AM_ROLE_CONFIG_RESOURCE);
      HoyaUtils.mergeEntries(rolemap, conf);
    }
    return rolemap;
  }


  @Override //Client
  public void preflightValidateClusterConfiguration(HoyaFileSystem hoyaFileSystem,
                                                    String clustername,
                                                    Configuration configuration,
                                                    AggregateConf instanceDefinition,
                                                    Path clusterDirPath,
                                                    Path generatedConfDirPath,
                                                    boolean secure) throws
                                                                    HoyaException,
                                                                    IOException {

    super.preflightValidateClusterConfiguration(hoyaFileSystem, clustername, configuration, instanceDefinition, clusterDirPath, generatedConfDirPath, secure);
    //add a check for the directory being writeable by the current user
    String
      dataPath = instanceDefinition.getInternalOperations()
                                   .getGlobalOptions()
                                   .getMandatoryOption(
                                     OptionKeys.INTERNAL_DATA_DIR_PATH);

    Path path = new Path(dataPath);
    hoyaFileSystem.verifyDirectoryWriteAccess(path);
    Path historyPath = new Path(clusterDirPath, HoyaKeys.HISTORY_DIR_NAME);
    hoyaFileSystem.verifyDirectoryWriteAccess(historyPath);
  }

  /**
   * The Hoya AM sets up all the dependency JARs above hoya.jar itself
   * {@inheritDoc}
   */
  public void prepareAMAndConfigForLaunch(HoyaFileSystem hoyaFileSystem,
                                                                Configuration serviceConf,
                                                                AbstractLauncher launcher,
                                                                AggregateConf instanceDescription,
                                                                Path originConfDirPath,
                                                                Path generatedConfDirPath,
                                                                Configuration clientConfExtras,
                                                                String libdir,
                                                                Path tempPath)
    throws IOException, HoyaException {
    
    Map<String, LocalResource> providerResources =
      new HashMap<String, LocalResource>();
    HoyaUtils.putJar(providerResources,
                     hoyaFileSystem,
                     JCommander.class,
                     tempPath,
                     libdir,
                     JCOMMANDER_JAR);
    HoyaUtils.putJar(providerResources,
                     hoyaFileSystem,
                     GsonBuilder.class,
                     tempPath,
                     libdir,
                     GSON_JAR);
    launcher.addLocalResources(providerResources);
    //also pick up all env variables from a map
    launcher.copyEnvVars(
      instanceDescription.getInternalOperations().getOrAddComponent(
        HoyaKeys.COMPONENT_AM));
  }

  /**
   * Update the AM resource with any local needs
   * @param capability capability to update
   */
  public void prepareAMResourceRequirements(MapOperations hoyaAM,
                                            Resource capability) {
    capability.setMemory(hoyaAM.getOptionInt(
      ResourceKeys.YARN_MEMORY,
      capability.getMemory()));
    capability.setVirtualCores(
      hoyaAM.getOptionInt(ResourceKeys.YARN_CORES, capability.getVirtualCores()));
  }
  
  /**
   * Extract any JVM options from the cluster specification and
   * add them to the command line
   * @param clusterSpec spec
   */
  public void addJVMOptions(AggregateConf aggregateConf,
                            CommandLineBuilder cmdLine) throws
                                                        BadConfigException {
    MapOperations hoyaAM =
      aggregateConf.getAppConfOperations().getMandatoryComponent(
        HoyaKeys.COMPONENT_AM);
    cmdLine.sysprop("java.net.preferIPv4Stack", "true");
    cmdLine.sysprop("java.awt.headless", "true");
    String heap = hoyaAM.getOption(RoleKeys.JVM_HEAP,
                                   DEFAULT_JVM_HEAP);
    cmdLine.setJVMHeap(heap);
    String jvmopts = hoyaAM.getOption(RoleKeys.JVM_OPTS, "");
    if (HoyaUtils.isSet(jvmopts)) {
      cmdLine.add(jvmopts);
    }
  }


  @Override
  public void prepareInstanceConfiguration(AggregateConf aggregateConf) throws
                                                                        HoyaException,
                                                                        IOException {
    mergeTemplates(aggregateConf,
                   INTERNAL_JSON, RESOURCES_JSON, APPCONF_JSON
                  );
  }
}
