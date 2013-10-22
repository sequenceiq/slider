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

package org.apache.hadoop.hoya.yarn.providers.accumulo

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hoya.api.ClusterDescription
import org.apache.hadoop.hoya.api.RoleKeys
import org.apache.hadoop.hoya.providers.accumulo.AccumuloConfigFileOptions
import org.apache.hadoop.hoya.providers.accumulo.AccumuloKeys
import org.apache.hadoop.hoya.yarn.Arguments
import org.apache.hadoop.hoya.yarn.KeysForTests
import org.apache.hadoop.hoya.yarn.client.HoyaClient
import org.apache.hadoop.hoya.yarn.cluster.YarnMiniClusterTestBase
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Assume
import org.apache.accumulo.core.client.ZooKeeperInstance;

/**
 * test base for all hbase clusters
 */
@CompileStatic
@Slf4j
public class AccumuloTestBase extends YarnMiniClusterTestBase {

  public static final int ACCUMULO_CLUSTER_STARTUP_TIME = 3 * 60 * 1000
  public static final int ACCUMULO_CLUSTER_STOP_TIME = 1 * 60 * 1000

  /**
   * The time to sleep before trying to talk to the HBase Master and
   * expect meaningful results.
   */
  public static final int ACCUMULO_CLUSTER_STARTUP_TO_LIVE_TIME = ACCUMULO_CLUSTER_STARTUP_TIME


  @Override
  public String getTestConfigurationPath() {
    return "src/main/resources/" + AccumuloKeys.CONF_RESOURCE; 
  }

  @Override
  void setup() {
    super.setup()
    assumeArchiveDefined();
    assumeServiceHome();
    YarnConfiguration conf = testConfiguration
    assumeOtherSettings(conf)
  }

  /**
   * Teardown 
   */
  @Override
  void teardown() {
    super.teardown();
    killAllAccumuloProcesses();
  }
  
  void killAllAccumuloProcesses() {
    killJavaProcesses("Main", 9)
  }

  /**
   * Fetch the current hbase site config from the Hoya AM, from the 
   * <code>hBaseClientProperties</code> field of the ClusterDescription
   * @param hoyaClient client
   * @param clustername name of the cluster
   * @return the site config
   */
  public Configuration fetchClientSiteConfig(HoyaClient hoyaClient) {
    ClusterDescription status = hoyaClient.clusterDescription;
    Configuration siteConf = new Configuration(false)
    status.clientProperties.each { String key, String val ->
      siteConf.set(key, val, "hoya cluster");
    }
    return siteConf;
  }

  public String getServiceHome() {
    YarnConfiguration conf = testConfiguration
    String hbaseHome = conf.getTrimmed(KeysForTests.HOYA_TEST_ACCUMULO_HOME)
    return hbaseHome
  }

  public String getArchiveKey() {
    YarnConfiguration conf = testConfiguration
    return conf.getTrimmed(KeysForTests.HOYA_TEST_ACCUMULO_TAR)
  }

  public void assumeArchiveDefined() {
    String archive = archiveKey
    Assume.assumeTrue("Archive conf option not set " + KeysForTests.HOYA_TEST_ACCUMULO_TAR,
                      archive != null && archive != "")
  }

  /**
   * Assume that HBase home is defined. This does not check that the
   * path is valid -that is expected to be a failure on tests that require
   * HBase home to be set.
   */
  public void assumeServiceHome() {
    Assume.assumeTrue("Service home conf option not set " + KeysForTests.HOYA_TEST_ACCUMULO_HOME,
                      serviceHome != null && serviceHome != "")
  }
  public void assumeOtherSettings(YarnConfiguration conf) {
    assumeConfOptionSet(conf, AccumuloKeys.OPTION_ZK_HOME)
  }


  /**
   * Get the arguments needed to point to HBase for these tests
   * @return
   */
  public List<String> getImageCommands() {
    if (switchToImageDeploy) {
      assert archiveKey
      File f = new File(archiveKey)
      assert f.exists()
      return [Arguments.ARG_IMAGE, f.toURI().toString()]
    } else {
      assert serviceHome
      assert new File(serviceHome).exists();
      return [Arguments.ARG_APP_HOME, serviceHome]
    }
  }

  /**
   * Create a full cluster with a master & the requested no. of region servers
   * @param clustername cluster name
   * @param tablets # of nodes
   * @param extraArgs list of extra args to add to the creation command
   * @param deleteExistingData should the data of any existing cluster
   * of this name be deleted
   * @param blockUntilRunning block until the AM is running
   * @return launcher which will have executed the command.
   */
  public ServiceLauncher createAccCluster(String clustername, int tablets, List<String> extraArgs, boolean deleteExistingData, boolean blockUntilRunning) {
    Map<String, Integer> roles = [
        (AccumuloKeys.ROLE_MASTER): 1,
        (AccumuloKeys.ROLE_TABLET): tablets,
    ];
    return createAccCluster(clustername, roles, extraArgs, deleteExistingData, blockUntilRunning);

  }

  /**
   * Create an accumulo cluster
   * @param clustername
   * @param roles
   * @param extraArgs
   * @param deleteExistingData
   * @param blockUntilRunning
   * @return the cluster launcher
   */
  public ServiceLauncher createAccCluster(String clustername, Map<String, Integer> roles, List<String> extraArgs, boolean deleteExistingData, boolean blockUntilRunning) {
    extraArgs << Arguments.ARG_PROVIDER << AccumuloKeys.PROVIDER_ACCUMULO;

    YarnConfiguration conf = testConfiguration

    def clusterOps = [
        (AccumuloKeys.OPTION_ZK_HOME): conf.getTrimmed(AccumuloKeys.OPTION_ZK_HOME),
        (AccumuloKeys.OPTION_HADOOP_HOME): conf.getTrimmed(AccumuloKeys.OPTION_HADOOP_HOME),
    ]

    extraArgs << Arguments.ARG_ROLEOPT << AccumuloKeys.ROLE_MASTER << RoleKeys.APP_INFOPORT << AccumuloConfigFileOptions.MASTER_PORT_CLIENT_DEFAULT
    extraArgs << Arguments.ARG_ROLEOPT << AccumuloKeys.ROLE_MONITOR << RoleKeys.APP_INFOPORT << AccumuloConfigFileOptions.MONITOR_PORT_CLIENT_DEFAULT

    return createHoyaCluster(clustername,
                             roles,
                             extraArgs,
                             deleteExistingData,
                             blockUntilRunning, 
                             clusterOps)
  }

  public void addOption(List<String> extraArgs, YarnConfiguration conf, String option) {
    assert conf.getTrimmed(option);
    extraArgs << Arguments.ARG_OPTION << option << conf.getTrimmed(option)
  }
  

  def getAccClusterStatus() {
    ZooKeeperInstance instance = new ZooKeeperInstance("", "localhost:4");
    instance.getConnector("user", "pass").instanceOperations().getTabletServers();
  }

  public def fetchWebPage(String url) {
    def client = new HttpClient(new MultiThreadedHttpConnectionManager());
    client.httpConnectionManager.params.connectionTimeout = 10000;
    GetMethod get = new GetMethod(url);

    get.followRedirects = true;
    int resultCode = client.executeMethod(get);
    String body = get.responseBodyAsString;
    return body;
  }
  
  
  public def fetchLocalPage(int port, String page) {
    String url = "http://localhost:" + AccumuloConfigFileOptions.MONITOR_PORT_CLIENT_DEFAULT + page
    String response = fetchWebPage(url)
    
  }
}
