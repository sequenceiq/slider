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
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

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
  
  /**
   * Find a jar that contains a class of the same name, if any. It will return
   * a jar file, even if that is not the first thing on the class path that
   * has a class with the same name. Looks first on the classpath and then in
   * the <code>packagedClasses</code> map.
   * @param my_class the class to find.
   * @return a jar file that contains the class, or null.
   * @throws IOException
   */
  private static String findContainingJar(Class<?> my_class, Map<String, String> packagedClasses)
      throws IOException {
    ClassLoader loader = my_class.getClassLoader();
    String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";

    // first search the classpath
    for (Enumeration<URL> itr = loader.getResources(class_file); itr.hasMoreElements();) {
      URL url = itr.nextElement();
      if ("jar".equals(url.getProtocol())) {
        String toReturn = url.getPath();
        if (toReturn.startsWith("file:")) {
          toReturn = toReturn.substring("file:".length());
        }
        // URLDecoder is a misnamed class, since it actually decodes
        // x-www-form-urlencoded MIME type rather than actual
        // URL encoding (which the file path has). Therefore it would
        // decode +s to ' 's which is incorrect (spaces are actually
        // either unencoded or encoded as "%20"). Replace +s first, so
        // that they are kept sacred during the decoding process.
        toReturn = toReturn.replaceAll("\\+", "%2B");
        toReturn = URLDecoder.decode(toReturn, "UTF-8");
        return toReturn.replaceAll("!.*$", "");
      }
    }

    // now look in any jars we've packaged using JarFinder. Returns null when
    // no jar is found.
    return packagedClasses.get(class_file);
  }

  /**
   * Invoke 'getJar' on a JarFinder implementation. Useful for some job
   * configuration contexts (HBASE-8140) and also for testing on MRv2. First
   * check if we have HADOOP-9426. Lacking that, fall back to the backport.
   * @param my_class the class to find.
   * @return a jar file that contains the class, or null.
   */
  private static String getJar(Class<?> my_class) {
    String ret = null;
    String hadoopJarFinder = "org.apache.hadoop.util.JarFinder";
    Class<?> jarFinder = null;
    try {
      log.debug("Looking for " + hadoopJarFinder + ".");
      jarFinder = Class.forName(hadoopJarFinder);
      log.debug(hadoopJarFinder + " found.");
      Method getJar = jarFinder.getMethod("getJar", Class.class);
      ret = (String) getJar.invoke(null, my_class);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("getJar invocation failed.", e);
    } catch (InvocationTargetException e) {
      // function was properly called, but threw it's own exception. Unwrap it
      // and pass it on.
      throw new RuntimeException(e.getCause());
    } catch (Exception e) {
      // toss all other exceptions, related to reflection failure
      throw new RuntimeException("getJar invocation failed.", e);
    }

    return ret;
  }

  /**
   * Add entries to <code>packagedClasses</code> corresponding to class files
   * contained in <code>jar</code>.
   * @param jar The jar who's content to list.
   * @param packagedClasses map[class -> jar]
   */
  private static void updateMap(String jar, Map<String, String> packagedClasses) throws IOException {
    ZipFile zip = null;
    try {
      zip = new ZipFile(jar);
      for (Enumeration<? extends ZipEntry> iter = zip.entries(); iter.hasMoreElements();) {
        ZipEntry entry = iter.nextElement();
        if (entry.getName().endsWith("class")) {
          packagedClasses.put(entry.getName(), jar);
        }
      }
    } finally {
      if (null != zip) zip.close();
    }
  }

  /**
   * If org.apache.hadoop.util.JarFinder is available (0.23+ hadoop), finds
   * the Jar for a class or creates it if it doesn't exist. If the class is in
   * a directory in the classpath, it creates a Jar on the fly with the
   * contents of the directory and returns the path to that Jar. If a Jar is
   * created, it is created in the system temporary directory. Otherwise,
   * returns an existing jar that contains a class of the same name. Maintains
   * a mapping from jar contents to the tmp jar created.
   * @param my_class the class to find.
   * @param fs the FileSystem with which to qualify the returned path.
   * @param packagedClasses a map of class name to path.
   * @return a jar file that contains the class.
   * @throws IOException
   */
  private static Path findOrCreateJar(Class<?> my_class, FileSystem fs,
      Map<String, String> packagedClasses)
  throws IOException {
    // attempt to locate an existing jar for the class.
    String jar = findContainingJar(my_class, packagedClasses);
    if (null == jar || jar.isEmpty()) {
      jar = getJar(my_class);
      updateMap(jar, packagedClasses);
    }

    if (null == jar || jar.isEmpty()) {
      throw new IOException("Cannot locate resource for class " + my_class.getName());
    }

    log.debug(String.format("For class %s, using jar %s", my_class.getName(), jar));
    return new Path(jar).makeQualified(fs);
  }
  
  /**
   * Add the jars containing the given classes to the job's configuration
   * such that JobClient will ship them to the cluster and add them to
   * the DistributedCache.
   */
  static void addDependencyJars(Configuration conf,
      Class<?>... classes) throws IOException {

    FileSystem localFs = FileSystem.getLocal(conf);
    Set<String> jars = new HashSet<String>();
    // Add jars that are already in the tmpjars variable
    jars.addAll(conf.getStringCollection("tmpjars"));

    // add jars as we find them to a map of contents jar name so that we can avoid
    // creating new jars for classes that have already been packaged.
    Map<String, String> packagedClasses = new HashMap<String, String>();

    // Add jars containing the specified classes
    for (Class<?> clazz : classes) {
      if (clazz == null) continue;

      Path path = findOrCreateJar(clazz, localFs, packagedClasses);
      if (path == null) {
        log.warn("Could not find jar for class " + clazz +
                 " in order to ship it to the cluster.");
        continue;
      }
      if (!localFs.exists(path)) {
        log.warn("Could not validate jar file " + path + " for class "
                 + clazz);
        continue;
      }
      jars.add(path.toString());
    }
    if (jars.isEmpty()) return;

    conf.set("tmpjars",
             StringUtils.arrayToString(jars.toArray(new String[0])));
  }

  /**
   * Add HBase and its dependencies (only) to the job configuration.
   * <p>
   * This is intended as a low-level API, facilitating code reuse between this
   * class and its mapred counterpart. It also of use to extenral tools that
   * need to build a MapReduce job that interacts with HBase but want
   * fine-grained control over the jars shipped to the cluster.
   * </p>
   * @param conf The Configuration object to extend with dependencies.
   * @see org.apache.hadoop.hbase.mapred.TableMapReduceUtil
   * @see <a href="https://issues.apache.org/jira/browse/PIG-3285">PIG-3285</a>
   */
  public static void addHBaseDependencyJars(Configuration conf) throws IOException {
    addDependencyJars(conf,
      // explicitly pull a class from each module
      org.apache.hadoop.hbase.HConstants.class,                      // hbase-common
      org.apache.hadoop.hbase.protobuf.generated.ClientProtos.class, // hbase-protocol
      org.apache.hadoop.hbase.client.Put.class,                      // hbase-client
      // pull necessary dependencies
      org.apache.zookeeper.ZooKeeper.class,
      com.google.protobuf.Message.class,
      com.google.common.collect.Lists.class);
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
      log.debug("Configuration came from {}",
                siteConf.get(HoyaXmlConfKeys.KEY_HOYA_TEMPLATE_ORIGIN));
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
    addHBaseDependencyJars(siteConf);
    
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
