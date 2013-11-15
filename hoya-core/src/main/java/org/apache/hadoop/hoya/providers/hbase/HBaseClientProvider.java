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

package org.apache.hadoop.hoya.providers.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.hoya.HostAndPort;
import org.apache.hadoop.hoya.HoyaKeys;
import org.apache.hadoop.hoya.HoyaXmlConfKeys;
import org.apache.hadoop.hoya.api.ClusterDescription;
import org.apache.hadoop.hoya.api.OptionKeys;
import org.apache.hadoop.hoya.api.RoleKeys;
import org.apache.hadoop.hoya.exceptions.BadConfigException;
import org.apache.hadoop.hoya.exceptions.HoyaException;
import org.apache.hadoop.hoya.providers.ClientProvider;
import org.apache.hadoop.hoya.providers.ProviderCore;
import org.apache.hadoop.hoya.providers.ProviderRole;
import org.apache.hadoop.hoya.providers.ProviderUtils;
import org.apache.hadoop.hoya.servicemonitor.HttpProbe;
import org.apache.hadoop.hoya.servicemonitor.MonitorKeys;
import org.apache.hadoop.hoya.servicemonitor.Probe;
import org.apache.hadoop.hoya.tools.ConfigHelper;
import org.apache.hadoop.hoya.tools.HoyaUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class implements  the client-side aspects
 * of an HBase Cluster
 */
public class HBaseClientProvider extends Configured implements
                                                          ProviderCore,
                                                          HBaseKeys, HoyaKeys,
                                                          ClientProvider,
                                                          HBaseConfigFileOptions {

  private static class ClientProviderAbortable implements Abortable {
    @Override
    public void abort(String why, Throwable e) {
    }
    @Override
    public boolean isAborted() {
      return false;
    }
  }
  protected static final Logger log =
    LoggerFactory.getLogger(HBaseClientProvider.class);
  protected static final String NAME = "hbase";
  private static final ProviderUtils providerUtils = new ProviderUtils(log);
  private MasterAddressTracker masterTracker = null;
  private Configuration conf;

  protected HBaseClientProvider(Configuration conf) {
    super(conf);
    this.conf = create(conf);
    try {
      Abortable abortable = new ClientProviderAbortable();
      ZooKeeperWatcher zkw = new ZooKeeperWatcher(this.conf, "HBaseClient", abortable);
      masterTracker = new MasterAddressTracker(zkw, abortable);
    } catch (IOException ioe) {
      log.error("Couldn't instantiate ZooKeeperWatcher", ioe);
    }
  }


  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public List<Probe> createProbes(String urlStr, Configuration config, int timeout) 
      throws IOException {
    List<Probe> probes = new ArrayList<Probe>();
    String prefix = "";
    URL url = null;
    if (urlStr != null && !urlStr.startsWith("http") && urlStr.contains("/proxy/")) {
      if (!UserGroupInformation.isSecurityEnabled()) {
        prefix = "http://proxy/relay/";
      } else {
        prefix = "https://proxy/relay/";
      }
    }
    try {
      url = new URL(prefix + urlStr);
    } catch (MalformedURLException mue) {
      log.error("tracking url: " + prefix + urlStr + " is malformed");
    }
    if (url != null) {
      log.info("tracking url: " + url);
      HttpURLConnection connection = null;
      try {
        connection = HttpProbe.getConnection(url, timeout);
        // see if the host is reachable
        connection.getResponseCode();

        HttpProbe probe = new HttpProbe(url, timeout,
          MonitorKeys.WEB_PROBE_DEFAULT_CODE, MonitorKeys.WEB_PROBE_DEFAULT_CODE, config);
        probes.add(probe);
      } catch (UnknownHostException uhe) {
        log.error("host unknown: " + url);
      } finally {
        if (connection != null) {
          connection.disconnect();
          connection = null;
        }
      }
    }
    return probes;
  }

  @Override
  public HostAndPort getMasterAddress() throws IOException, KeeperException {
    // masterTracker receives notification from zookeeper on current master
    ServerName sn = masterTracker.getMasterAddress();
    log.debug("getMasterAddress " + sn + ", quorum=" + this.conf.get(HConstants.ZOOKEEPER_QUORUM));
    if (sn == null) return null;
    return new HostAndPort(sn.getHostname(), sn.getPort());
  }
  
  private Collection<HostAndPort> serverNameToHostAndPort(Collection<ServerName> servers) {
    Collection<HostAndPort> col = new ArrayList<HostAndPort>();
    if (servers == null || servers.isEmpty()) return col;
    for (ServerName sn : servers) {
      col.add(new HostAndPort(sn.getHostname(), sn.getPort()));
    }
    return col;
  }

  @Override
  public Configuration create(Configuration conf) {
    return HBaseConfiguration.create(conf);
  }

  @Override
  public Collection<HostAndPort> listDeadServers(Configuration conf)  throws IOException {
    HConnection hbaseConnection = HConnectionManager.createConnection(conf);
    HBaseAdmin hBaseAdmin = new HBaseAdmin(hbaseConnection);
    try {
      ClusterStatus cs = hBaseAdmin.getClusterStatus();
      return serverNameToHostAndPort(cs.getDeadServerNames());
    } finally {
      hBaseAdmin.close();
      hbaseConnection.close();
    }
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
  public Map<String, String> getDefaultClusterOptions() {
    HashMap<String, String> site = new HashMap<String, String>();
    site.put(OptionKeys.APPLICATION_VERSION, HBaseKeys.VERSION);
    return site;
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
                                                                       HoyaException {
    Map<String, String> rolemap = new HashMap<String, String>();
    rolemap.put(RoleKeys.ROLE_NAME, rolename);
    rolemap.put(RoleKeys.YARN_CORES, Integer.toString(RoleKeys.DEF_YARN_CORES));
    rolemap.put(RoleKeys.YARN_MEMORY, Integer.toString(RoleKeys.DEF_YARN_MEMORY));
    if (rolename.equals(HBaseKeys.ROLE_WORKER)) {
      rolemap.put(RoleKeys.APP_INFOPORT, DEFAULT_HBASE_WORKER_INFOPORT);
      rolemap.put(RoleKeys.JVM_HEAP, DEFAULT_HBASE_WORKER_HEAP);
    } else if (rolename.equals(HBaseKeys.ROLE_MASTER)) {
      rolemap.put(RoleKeys.ROLE_INSTANCES, "1");
      rolemap.put(RoleKeys.APP_INFOPORT, DEFAULT_HBASE_MASTER_INFOPORT);
      rolemap.put(RoleKeys.JVM_HEAP, DEFAULT_HBASE_MASTER_HEAP);
    }
    return rolemap;
  }

  /**
   * Build the conf dir from the service arguments, adding the hbase root
   * to the FS root dir.
   * This the configuration used by HBase directly
   * @param clusterSpec this is the cluster specification used to define this
   * @return a map of the dynamic bindings for this Hoya instance
   */
  public Map<String, String> buildSiteConfFromSpec(ClusterDescription clusterSpec)
    throws BadConfigException {

    Map<String, String> master = clusterSpec.getMandatoryRole(
      HBaseKeys.ROLE_MASTER);

    Map<String, String> worker = clusterSpec.getMandatoryRole(
      HBaseKeys.ROLE_WORKER);

    Map<String, String> sitexml = new HashMap<String, String>();
    
    //map all cluster-wide site. options
    providerUtils.propagateSiteOptions(clusterSpec, sitexml);
/*
  //this is where we'd do app-indepdenent keytabs

    String keytab =
      clusterSpec.getOption(OptionKeys.OPTION_KEYTAB_LOCATION, "");
    
*/


    sitexml.put(KEY_HBASE_CLUSTER_DISTRIBUTED, "true");
    sitexml.put(KEY_HBASE_MASTER_PORT, "0");

    sitexml.put(KEY_HBASE_MASTER_INFO_PORT, master.get(
      RoleKeys.APP_INFOPORT));
    sitexml.put(KEY_HBASE_ROOTDIR,
                clusterSpec.dataPath);
    sitexml.put(KEY_REGIONSERVER_INFO_PORT,
                worker.get(RoleKeys.APP_INFOPORT));
    sitexml.put(KEY_REGIONSERVER_PORT, "0");
    providerUtils.propagateOption(clusterSpec, OptionKeys.ZOOKEEPER_PATH,
                                  sitexml, KEY_ZNODE_PARENT);
    providerUtils.propagateOption(clusterSpec, OptionKeys.ZOOKEEPER_PORT,
                                  sitexml, KEY_ZOOKEEPER_PORT);
    providerUtils.propagateOption(clusterSpec, OptionKeys.ZOOKEEPER_HOSTS,
                                  sitexml, KEY_ZOOKEEPER_QUORUM);

    return sitexml;
  }

  /**
   * Build time review and update of the cluster specification
   * @param clusterSpec spec
   */
  @Override // Client
  public void reviewAndUpdateClusterSpec(ClusterDescription clusterSpec) throws
                                                                         HoyaException{

    validateClusterSpec(clusterSpec);
  }


  @Override //Client
  public void preflightValidateClusterConfiguration(ClusterDescription clusterSpec,
                                                    FileSystem clusterFS,
                                                    Path generatedConfDirPath,
                                                    boolean secure) throws
                                                                    HoyaException,
                                                                    IOException {
    validateClusterSpec(clusterSpec);
    Path templatePath = new Path(generatedConfDirPath, HBaseKeys.SITE_XML);
    //load the HBase site file or fail
    Configuration siteConf = ConfigHelper.loadConfiguration(clusterFS,
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
      providerUtils.verifyOptionSet(siteConf, KEY_HBASE_CLUSTER_DISTRIBUTED,
                                    false);
      providerUtils.verifyOptionSet(siteConf, KEY_HBASE_ROOTDIR, false);
      providerUtils.verifyOptionSet(siteConf, KEY_ZNODE_PARENT, false);
      providerUtils.verifyOptionSet(siteConf, KEY_ZOOKEEPER_QUORUM, false);
      providerUtils.verifyOptionSet(siteConf, KEY_ZOOKEEPER_PORT, false);
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
        providerUtils.verifyOptionSet(siteConf, KEY_MASTER_KERBEROS_PRINCIPAL,
                                      false);
        providerUtils.verifyOptionSet(siteConf, KEY_MASTER_KERBEROS_KEYTAB,
                                      false);
        providerUtils.verifyOptionSet(siteConf,
                                      KEY_REGIONSERVER_KERBEROS_PRINCIPAL,
                                      false);
        providerUtils.verifyOptionSet(siteConf,
                                      KEY_REGIONSERVER_KERBEROS_KEYTAB, false);
      }
    } catch (BadConfigException e) {
      //bad configuration, dump it

      log.error("Bad site configuration {} : {}", origin, e, e);
      log.info(ConfigHelper.dumpConfigToString(siteConf));
      throw e;
    }
  }

  /**
   * Validate the cluster specification. This can be invoked on both
   * server and client
   * @param clusterSpec
   */
  @Override // Client and Server
  public void validateClusterSpec(ClusterDescription clusterSpec) throws
                                                                  HoyaException {
    providerUtils.validateNodeCount(HBaseKeys.ROLE_WORKER,
                                    clusterSpec.getDesiredInstanceCount(
                                      HBaseKeys.ROLE_WORKER,
                                      0), 0, -1);


    providerUtils.validateNodeCount(HBaseKeys.ROLE_MASTER,
                                    clusterSpec.getDesiredInstanceCount(
                                      HBaseKeys.ROLE_MASTER,
                                      0),
                                    0,
                                    -1);
  }
  

  @Override
  public Map<String, LocalResource> prepareAMAndConfigForLaunch(FileSystem clusterFS,
                                                                Configuration serviceConf,
                                                                ClusterDescription clusterSpec,
                                                                Path originConfDirPath,
                                                                Path generatedConfDirPath,
                                                                Configuration clientConfExtras) throws
                                                                                           IOException,
                                                                                           BadConfigException {
    //load in the template site config
    log.debug("Loading template configuration from {}", originConfDirPath);
    Configuration siteConf = ConfigHelper.loadTemplateConfiguration(
      serviceConf,
      originConfDirPath,
      HBaseKeys.SITE_XML,
      HBaseKeys.HBASE_TEMPLATE_RESOURCE);
    
    if (log.isDebugEnabled()) {
      log.debug("Configuration came from {}", siteConf.get(
        HoyaXmlConfKeys.KEY_HOYA_TEMPLATE_ORIGIN));
      ConfigHelper.dumpConf(siteConf);
    }
    //construct the cluster configuration values
    Map<String, String> clusterConfMap = buildSiteConfFromSpec(clusterSpec);
    
    //merge them
    ConfigHelper.addConfigMap(siteConf, clusterConfMap, "HBase Provider");

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
    Map<String, LocalResource> confResources;
    confResources = HoyaUtils.submitDirectory(clusterFS,
                                              generatedConfDirPath,
                                              HoyaKeys.PROPAGATED_CONF_DIR_NAME);
    
    //now set up the directory for writing by the user
    providerUtils.createDataDirectory(clusterSpec, getConf());
/* TODO: anything else to set up node security
    if (UserGroupInformation.isSecurityEnabled()) {
      //secure mode
      UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
      String shortname = loginUser.getShortUserName();
      String masterPrincipal = siteConf.get(KEY_MASTER_KERBEROS_PRINCIPAL);

      Path hbaseData = new Path(clusterSpec.dataPath);
      if (clusterFS.exists(hbaseData)) {
        throw new FileNotFoundException(
          "HBase data directory not found: " + hbaseData);
      }
        
      FsPermission permission = new FsPermission(
        FsAction.ALL, FsAction.ALL,FsAction.EXECUTE
      );
      clusterFS.setPermission(hbaseData, permission);
    }*/
    
    return confResources;
  }

  /**
   * Update the AM resource with any local needs
   * @param capability capability to update
   */
  @Override
  public void prepareAMResourceRequirements(ClusterDescription clusterSpec,
                                            Resource capability) {
    //no-op unless you want to add more memory
    capability.setMemory(clusterSpec.getRoleOptInt(
      HBaseKeys.ROLE_MASTER,
      RoleKeys.YARN_MEMORY,
      capability.getMemory()));
    capability.setVirtualCores(1);
  }


  /**
   * Any operations to the service data before launching the AM
   * @param clusterSpec cspec
   * @param serviceData map of service data
   */
  @Override  //Client
  public void prepareAMServiceData(ClusterDescription clusterSpec,
                                   Map<String, ByteBuffer> serviceData) {
    
  }


  /**
   * Get the path to hbase home
   * @return the hbase home path
   */
  public  File buildHBaseBinPath(ClusterDescription cd) {
    return new File(buildHBaseDir(cd),
                                HBaseKeys.HBASE_SCRIPT);
  }

  public  File buildHBaseDir(ClusterDescription cd) {
    String archiveSubdir = getHBaseVersion(cd);
    return providerUtils.buildImageDir(cd, archiveSubdir);
  }

  public String getHBaseVersion(ClusterDescription cd) {
    return cd.getOption(OptionKeys.APPLICATION_VERSION,
                                        HBaseKeys.VERSION);
  }

}
