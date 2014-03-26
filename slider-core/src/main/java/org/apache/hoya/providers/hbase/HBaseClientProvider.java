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
import org.apache.hoya.HoyaKeys;
import org.apache.hoya.HoyaXmlConfKeys;
import org.apache.hoya.api.OptionKeys;
import org.apache.hoya.core.conf.AggregateConf;
import org.apache.hoya.core.conf.ConfTreeOperations;
import org.apache.hoya.core.conf.MapOperations;
import org.apache.hoya.core.launch.AbstractLauncher;
import org.apache.hoya.exceptions.BadCommandArgumentsException;
import org.apache.hoya.exceptions.BadConfigException;
import org.apache.hoya.exceptions.HoyaException;
import org.apache.hoya.providers.AbstractClientProvider;
import org.apache.hoya.providers.ProviderRole;
import org.apache.hoya.tools.ConfigHelper;
import org.apache.hoya.tools.HoyaFileSystem;
import org.apache.hoya.tools.HoyaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class implements  the client-side aspects
 * of an HBase Cluster
 */
public class HBaseClientProvider extends AbstractClientProvider implements
                                                          HBaseKeys, HoyaKeys,
                                                          HBaseConfigFileOptions {

  
  protected static final Logger log =
    LoggerFactory.getLogger(HBaseClientProvider.class);
  protected static final String NAME = "hbase";
  private static final String INSTANCE_RESOURCE_BASE = PROVIDER_RESOURCE_BASE_ROOT +
                                                       "hbase/instance/";


  protected HBaseClientProvider(Configuration conf) {
    super(conf);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public List<ProviderRole> getRoles() {
    return HBaseRoles.getRoles();
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
      "org/apache/hoya/providers/hbase/hbase.xml");
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
                                                                       HoyaException, IOException {
    Map<String, String> rolemap = new HashMap<String, String>();
    if (rolename.equals(HBaseKeys.ROLE_MASTER)) {
      // master role
      Configuration conf = ConfigHelper.loadMandatoryResource(
        PROVIDER_RESOURCE_BASE +"hbase/role-hbase-master.xml");
      HoyaUtils.mergeEntries(rolemap, conf);
    } else if (rolename.equals(HBaseKeys.ROLE_WORKER)) {
      // worker settings
      Configuration conf = ConfigHelper.loadMandatoryResource(
        PROVIDER_RESOURCE_BASE +"hbase/role-hbase-worker.xml");
      HoyaUtils.mergeEntries(rolemap, conf);
    }
    return rolemap;
  }

  @Override
  public void prepareInstanceConfiguration(AggregateConf aggregateConf) throws
                                                              HoyaException,
                                                              IOException {
    String resourceTemplate = INSTANCE_RESOURCE_BASE + "resources.json";
    String appConfTemplate = INSTANCE_RESOURCE_BASE + "appconf.json";
    mergeTemplates(aggregateConf, null, resourceTemplate, appConfTemplate);
  }

  /**
   * Build the hdfs-site.xml file
   * This the configuration used by HBase directly
   * @param instanceDescription this is the cluster specification used to define this
   * @return a map of the dynamic bindings for this Hoya instance
   */
  public Map<String, String> buildSiteConfFromInstance(
    AggregateConf instanceDescription)
    throws BadConfigException {


    ConfTreeOperations appconf =
      instanceDescription.getAppConfOperations();

    MapOperations globalAppOptions = appconf.getGlobalOptions();
    MapOperations globalInstanceOptions = instanceDescription.getInternalOperations().getGlobalOptions();
    MapOperations master = appconf.getMandatoryComponent(HBaseKeys.ROLE_MASTER);

    MapOperations worker = appconf.getMandatoryComponent(HBaseKeys.ROLE_WORKER);
    
    Map<String, String> sitexml = new HashMap<String, String>();

    //map all cluster-wide site. options
    providerUtils.propagateSiteOptions(globalAppOptions, sitexml);
/*
  //this is where we'd do app-indepdenent keytabs

    String keytab =
      clusterSpec.getOption(OptionKeys.OPTION_KEYTAB_LOCATION, "");
    
*/


    sitexml.put(KEY_HBASE_ROOTDIR,
                globalInstanceOptions.getMandatoryOption(
                  OptionKeys.INTERNAL_DATA_DIR_PATH) );
    providerUtils.propagateOption(globalAppOptions, OptionKeys.ZOOKEEPER_PATH,
                                  sitexml, KEY_ZNODE_PARENT);
    providerUtils.propagateOption(globalAppOptions, OptionKeys.ZOOKEEPER_PORT,
                                  sitexml, KEY_ZOOKEEPER_PORT);
    providerUtils.propagateOption(globalAppOptions, OptionKeys.ZOOKEEPER_HOSTS,
                                  sitexml, KEY_ZOOKEEPER_QUORUM);

    return sitexml;
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
    super.preflightValidateClusterConfiguration(hoyaFileSystem, clustername,
                                                configuration,
                                                instanceDefinition,
                                                clusterDirPath,
                                                generatedConfDirPath, secure);

    Path templatePath = new Path(generatedConfDirPath, HBaseKeys.SITE_XML);
    //load the HBase site file or fail
    Configuration siteConf = ConfigHelper.loadConfiguration(hoyaFileSystem.getFileSystem(),
                                                            templatePath);

    //core customizations
    validateHBaseSiteXML(siteConf, secure, templatePath.toString());

  }

  /**
   * Validate the hbase-site.xml values
   * @param siteConf site config
   * @param secure secure flag
   * @param origin origin for exceptions
   * @throws BadConfigException if a config is missing/invalid
   */
  public void validateHBaseSiteXML(Configuration siteConf,
                                    boolean secure,
                                    String origin) throws BadConfigException {
    try {
      HoyaUtils.verifyOptionSet(siteConf, KEY_HBASE_CLUSTER_DISTRIBUTED,
                                false);
      HoyaUtils.verifyOptionSet(siteConf, KEY_HBASE_ROOTDIR, false);
      HoyaUtils.verifyOptionSet(siteConf, KEY_ZNODE_PARENT, false);
      HoyaUtils.verifyOptionSet(siteConf, KEY_ZOOKEEPER_QUORUM, false);
      HoyaUtils.verifyOptionSet(siteConf, KEY_ZOOKEEPER_PORT, false);
      int zkPort =
        siteConf.getInt(HBaseConfigFileOptions.KEY_ZOOKEEPER_PORT, 0);
      if (zkPort == 0) {
        throw new BadConfigException(
          "ZK port property not provided at %s in configuration file %s",
          HBaseConfigFileOptions.KEY_ZOOKEEPER_PORT,
          siteConf);
      }

      if (secure) {
        //better have the secure cluster definition up and running
        HoyaUtils.verifyOptionSet(siteConf, KEY_MASTER_KERBEROS_PRINCIPAL,
                                  false);
        HoyaUtils.verifyOptionSet(siteConf, KEY_MASTER_KERBEROS_KEYTAB,
                                  false);
        HoyaUtils.verifyOptionSet(siteConf,
                                  KEY_REGIONSERVER_KERBEROS_PRINCIPAL,
                                  false);
        HoyaUtils.verifyOptionSet(siteConf,
                                  KEY_REGIONSERVER_KERBEROS_KEYTAB, false);
      }
    } catch (BadConfigException e) {
      //bad configuration, dump it

      log.error("Bad site configuration {} : {}", origin, e, e);
      log.info(ConfigHelper.dumpConfigToString(siteConf));
      throw e;
    }
  }

  private static Set<String> knownRoleNames = new HashSet<String>();
  static {
    List<ProviderRole> roles = HBaseRoles.getRoles();
    knownRoleNames.add(HoyaKeys.COMPONENT_AM);
    for (ProviderRole role : roles) {
      knownRoleNames.add(role.name);
    }
  }
  
  /**
   * Validate the instance definition.
   * @param clusterSpec
   */
  @Override
  public void validateInstanceDefinition(AggregateConf instanceDefinition) throws
                                                                           HoyaException {
    super.validateInstanceDefinition(instanceDefinition);
    ConfTreeOperations resources =
      instanceDefinition.getResourceOperations();
    Set<String> unknownRoles = resources.getComponentNames();
    unknownRoles.removeAll(knownRoleNames);
    if (!unknownRoles.isEmpty()) {
      throw new BadCommandArgumentsException("Unknown component: %s",
                                             unknownRoles.iterator().next());
    }
    providerUtils.validateNodeCount(instanceDefinition, HBaseKeys.ROLE_WORKER,
                                    0, -1);
    providerUtils.validateNodeCount(instanceDefinition, HBaseKeys.ROLE_MASTER,
                                    0, -1);

  }

  @Override
  public void prepareAMAndConfigForLaunch(HoyaFileSystem hoyaFileSystem,
                                          Configuration serviceConf,
                                          AbstractLauncher launcher,
                                          AggregateConf instanceDescription,
                                          Path originConfDirPath,
                                          Path generatedConfDirPath,
                                          Configuration clientConfExtras,
                                          String libdir,
                                          Path tempPath) throws
                                                         IOException,
                                                         HoyaException {
    //load in the template site config
    log.debug("Loading template configuration from {}", originConfDirPath);
    Configuration siteConf = ConfigHelper.loadTemplateConfiguration(
      serviceConf,
      originConfDirPath,
      HBaseKeys.SITE_XML,
      HBaseKeys.HBASE_TEMPLATE_RESOURCE);

    if (log.isDebugEnabled()) {
      log.debug("Configuration came from {}",
                siteConf.get(HoyaXmlConfKeys.KEY_HOYA_TEMPLATE_ORIGIN));
      ConfigHelper.dumpConf(siteConf);
    }
    //construct the cluster configuration values

    ConfTreeOperations appconf =
      instanceDescription.getAppConfOperations();

    
    Map<String, String> clusterConfMap = buildSiteConfFromInstance(
      instanceDescription);

    //merge them
    ConfigHelper.addConfigMap(siteConf,
                              clusterConfMap.entrySet(),
                              "HBase Provider");

    //now, if there is an extra client conf, merge it in too
    if (clientConfExtras != null) {
      ConfigHelper.mergeConfigurations(siteConf, clientConfExtras,
                                       "Hoya Client");
    }

    if (log.isDebugEnabled()) {
      log.debug("Merged Configuration");
      ConfigHelper.dumpConf(siteConf);
    }

    Path sitePath = ConfigHelper.saveConfig(serviceConf,
                                            siteConf,
                                            generatedConfDirPath,
                                            HBaseKeys.SITE_XML);

    log.debug("Saving the config to {}", sitePath);
    launcher.submitDirectory(generatedConfDirPath,
                             HoyaKeys.PROPAGATED_CONF_DIR_NAME);

  }


}
