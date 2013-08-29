/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hoya.yarn.cluster

import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem as HadoopFS
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.ClusterStatus
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.ServerName
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HConnection
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.hoya.HoyaExitCodes
import org.apache.hadoop.hoya.api.ClusterDescription
import org.apache.hadoop.hoya.api.ClusterNode
import org.apache.hadoop.hoya.api.OptionKeys
import org.apache.hadoop.hoya.exceptions.HoyaException
import org.apache.hadoop.hoya.exceptions.WaitTimeoutException
import org.apache.hadoop.hoya.providers.hbase.HBaseConfigFileOptions
import org.apache.hadoop.hoya.providers.hbase.HBaseKeys
import org.apache.hadoop.hoya.tools.ConfigHelper
import org.apache.hadoop.hoya.tools.Duration
import org.apache.hadoop.hoya.tools.HoyaUtils
import org.apache.hadoop.hoya.yarn.CommonArgs
import org.apache.hadoop.hoya.yarn.HoyaActions
import org.apache.hadoop.hoya.yarn.KeysForTests
import org.apache.hadoop.hoya.yarn.MicroZKCluster
import org.apache.hadoop.hoya.yarn.ZKIntegration
import org.apache.hadoop.hoya.yarn.client.ClientArgs
import org.apache.hadoop.hoya.yarn.client.HoyaClient
import org.apache.hadoop.service.ServiceOperations
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.MiniYARNCluster
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler
import org.apache.hadoop.yarn.service.launcher.ServiceLaunchException
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.apache.hadoop.yarn.service.launcher.ServiceLauncherBaseTest
import org.junit.After
import org.junit.Assume
import org.junit.Rule
import org.junit.rules.Timeout

import java.util.concurrent.ExecutorService
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Base class for mini cluster tests -creates a field for the
 * mini yarn cluster
 */
@CompileStatic
@Slf4j
public class YarnMiniClusterTestBase extends ServiceLauncherBaseTest
implements KeysForTests, HoyaExitCodes {

  /**
   * Mini YARN cluster only
   */
  public static final int CLUSTER_GO_LIVE_TIME = 3 * 60 * 1000
  public static final int HBASE_CLUSTER_STARTUP_TIME = 3 * 60 * 1000
  public static final int HBASE_CLUSTER_STOP_TIME = 1 * 60 * 1000
  
  /**
   * The time to sleep before trying to talk to the HBase Master and
   * expect meaningful results.
   */
  public static final int HBASE_CLUSTER_STARTUP_TO_LIVE_TIME = HBASE_CLUSTER_STARTUP_TIME
  public static final String HREGION = "HRegion"
  public static final List<String> HBASE_VERSION_COMMAND_SEQUENCE = [
      CommonArgs.ARG_OPTION, HBaseConfigFileOptions.OPTION_HBASE_MASTER_COMMAND, "version",
  ]

  protected MiniDFSCluster hdfsCluster
  protected MiniYARNCluster miniCluster;
  protected MicroZKCluster microZKCluster
  protected boolean switchToImageDeploy = false

  protected List<HoyaClient> clustersToTeardown = [];


  @Rule
  public final Timeout testTimeout = new Timeout(10*60*1000); 

  @After
  public void teardown() {
    describe("teardown")
    stopRunningClusters();
    stopMiniCluster();
    killAllRegionServers();
  }

  protected void addToTeardown(HoyaClient client) {
    clustersToTeardown << client;
  }
  /**
   * Stop any running cluster that has been added
   */
  public void stopRunningClusters() {
    clustersToTeardown.each { HoyaClient hoyaClient ->
      try {
        maybeStopCluster(hoyaClient, "");
      } catch (Exception e) {
        log.warn("While stopping cluster " + e, e);
      }
    }
  }
  
  public void stopMiniCluster() {
    Log l = LogFactory.getLog(this.getClass())
    ServiceOperations.stopQuietly(l, miniCluster)
    microZKCluster?.close();
    hdfsCluster?.shutdown();
  }
  
  protected YarnConfiguration createConfiguration() {
    return HoyaUtils.createConfiguration();
  }

  /**
   * Print a description with some markers to
   * indicate this is the test description
   * @param s
   */
  protected void describe(String s) {
    log.info("");
    log.info("===============================");
    log.info(s);
    log.info("===============================");
    log.info("");
  }

  public ZKIntegration createZKIntegrationInstance(String zkQuorum, String clusterName, boolean createClusterPath, boolean canBeReadOnly, int timeout) {
    AtomicBoolean connectedFlag = new AtomicBoolean(false)
    ZKIntegration zki = ZKIntegration.newInstance(zkQuorum,
                                                  USERNAME,
                                                  clusterName,
                                                  createClusterPath,
                                                  canBeReadOnly) {
      //connection callback
      synchronized (connectedFlag) {
        log.info("ZK binding callback received")
        connectedFlag.set(true)
        connectedFlag.notify()
      }
    }
    zki.init()
    //here the callback may or may not have occurred.
    //optionally wait for it
    if (timeout > 0) {
      waitForZKConnection(connectedFlag, timeout)
    }
    //if we get here, the binding worked
    log.info("Connected: ${zki}")
    return zki
  }

  /**
   * Wait for a flag to go true
   * @param connectedFlag
   */
  public void waitForZKConnection(AtomicBoolean connectedFlag, int timeout) {
    synchronized (connectedFlag) {
      if (!connectedFlag.get()) {
        log.info("waiting for ZK event")
        //wait a bit
        connectedFlag.wait(timeout)
      }
    }
    assert connectedFlag.get()
  }

  /**
   * Create and start a minicluster
   * @param name cluster/test name
   * @param conf configuration to use
   * @param noOfNodeManagers #of NMs
   * @param numLocalDirs #of local dirs
   * @param numLogDirs #of log dirs
   * @param startZK create a ZK micro cluster
   * @param startHDFS create an HDFS mini cluster
   */
  protected void createMiniCluster(String name,
                                   YarnConfiguration conf,
                                   int noOfNodeManagers,
                                   int numLocalDirs,
                                   int numLogDirs,
                                   boolean startZK,
                                   boolean startHDFS) {
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
    conf.setClass(YarnConfiguration.RM_SCHEDULER,
                  FifoScheduler.class, ResourceScheduler.class);
    HoyaUtils.patchConfiguration(conf)
    miniCluster = new MiniYARNCluster(name, noOfNodeManagers, numLocalDirs, numLogDirs)
    miniCluster.init(conf)
    miniCluster.start();
    //now the ZK cluster
    if (startZK) {
      createMicroZKCluster(conf)
    }
    if (startHDFS) {
      File baseDir = new File("./target/hdfs/$name").absoluteFile;
      //use file: to rm it recursively
      FileUtil.fullyDelete(baseDir)
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.absolutePath)
      MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf)
      hdfsCluster = builder.build()
    }

  }

  public void createMicroZKCluster(Configuration conf) {
    microZKCluster = new MicroZKCluster(new Configuration(conf))
    microZKCluster.createCluster();
  }

  /**
   * Create and start a minicluster
   * @param name cluster/test name
   * @param conf configuration to use
   * @param noOfNodeManagers #of NMs
   * @param numLocalDirs #of local dirs
   * @param numLogDirs #of log dirs
   * @param startZK create a ZK micro cluster
   * @param startHDFS create an HDFS mini cluster
   */
  protected void createMiniCluster(String name, YarnConfiguration conf, int noOfNodeManagers, boolean startZK) {
    createMiniCluster(name, conf, noOfNodeManagers, 1, 1, startZK, false)
  }

  /**
   * Launch the hoya client with the specific args
   * @param conf configuration
   * @param args arg list
   * @return the service launcher that launched it, containing exit codes
   * and the service itself
   */
  protected ServiceLauncher launchHoyaClient(Configuration conf, List args) {
    return launch(HoyaClient, conf, args);
  }

  /**
   * Launch the hoya client with the specific args against the MiniMR cluster
   * launcher ie expected to have successfully completed
   * @param conf configuration
   * @param args arg list
   * @return the return code
   */
  protected ServiceLauncher launchHoyaClientAgainstMiniMR(Configuration conf,
                                                          List args) {
    assert miniCluster != null
    ResourceManager rm = miniCluster.resourceManager
    log.info("Connecting to rm at ${rm}")

    if (!args.contains(ClientArgs.ARG_MANAGER)) {
      args += [ClientArgs.ARG_MANAGER, RMAddr]
    }
    ServiceLauncher launcher = execHoyaCommand(conf, args)
    assert launcher.serviceExitCode == 0
    return launcher;
  }
  
  /**
   * Launch the hoya client with the specific args; no validation
   * of return code takes place
   * @param conf configuration
   * @param args arg list
   * @return the return code
   */
  protected ServiceLauncher execHoyaCommand(Configuration conf,
                                                          List args) {
    ServiceLauncher launcher = launch(HoyaClient, conf, args);
    return launcher;
  }

  /**
   * Kill all the region servers
   * <code>
   *    jps -l | grep HRegion | awk '{print $1}' | kill -9
   *  </code>
   */
  public void killAllRegionServers() {
    killJavaProcesses(HREGION);
  }

  
  /**
   * Kill any java process with the given grep pattern
   * @param grepString string to grep for
   */
  public void killJavaProcesses(String grepString) {
    Process bash = ["bash", "-c", "jps -l | grep ${grepString} | awk '{print \$1}' | xargs kill -9"].execute()
    log.info(bash.text)
  }

   /**
   * List any java process with the given grep pattern
   * @param grepString string to grep for
   */
  public String lsJavaProcesses(String grepString) {
    Process bash = ["bash", "-c", "jps -l | grep ${grepString} | awk '{print \$1}' "].execute()
    return bash.text
  }

  
  public String getHBaseHome() {
    YarnConfiguration conf = getTestConfiguration()
    String hbaseHome = conf.getTrimmed(HOYA_TEST_HBASE_HOME)
    return hbaseHome
  }

  public String getHBaseArchive() {
    YarnConfiguration conf = getTestConfiguration()
    return conf.getTrimmed(HOYA_TEST_HBASE_TAR)
  }

  public void assumeHBaseArchive() {
    String hbaseArchive = getHBaseArchive()
    Assume.assumeTrue("Hbase Archive conf option not set " + HOYA_TEST_HBASE_TAR,
                      hbaseArchive != null && hbaseArchive != "")
  }

  /**
   * Get the arguments needed to point to HBase for these tests
   * @return
   */
  public List<String> getHBaseImageCommands() {
    if (switchToImageDeploy) {
      assert HBaseArchive
      File f = new File(HBaseArchive)
      assert f.exists()
      return [CommonArgs.ARG_IMAGE, f.toURI().toString()]
    } else {
      assert HBaseHome 
      assert new File(HBaseHome).exists();
      return [CommonArgs.ARG_APP_HOME, HBaseHome]
    }
  }


  public YarnConfiguration getTestConfiguration() {
    YarnConfiguration conf = createConfiguration()

    conf.addResource(HOYA_TEST)
    return conf
  }

  protected String getRMAddr() {
    assert miniCluster != null
    String addr = miniCluster.config.get(YarnConfiguration.RM_ADDRESS)
    assert addr != null;
    assert addr != "";
    return addr
  }

  void assertHasZKCluster() {
    assert microZKCluster != null
  }

  protected String getZKBinding() {
    if (!microZKCluster) {
      return "localhost:1"
    } else {
      return microZKCluster.zkBindingString
    }
  }

  protected int getZKPort() {
    return microZKCluster ? microZKCluster.port : HBaseConfigFileOptions.HBASE_ZK_PORT;
  }

  protected String getZKHosts() {
    return MicroZKCluster.HOSTS;
  }

  /**
   * return the default filesystem, which is HDFS if the miniDFS cluster is
   * up, file:// if not
   * @return a filesystem string to pass down
   */
  protected String getFsDefaultName() {
    if (hdfsCluster) {
      return "hdfs://localhost:${hdfsCluster.nameNodePort}/"
    } else {
      return "file:///"
    }
  }
  
  protected String getWaitTimeArg() {
    return WAIT_TIME_ARG;
  }
  
  protected String getWaitTime() {
    return WAIT_TIME;
  }

  /**
   * Create an AM without a master
   * @param clustername AM name
   * @param size # of nodes
   * @param deleteExistingData should any existing cluster data be deleted
   * @param blockUntilRunning block until the AM is running
   * @return launcher which will have executed the command.
   */
  public ServiceLauncher createMasterlessAM(String clustername, int size, boolean deleteExistingData, boolean blockUntilRunning) {
    return createHoyaCluster(clustername,
                             size,
                             [CommonArgs.ARG_MASTERS, "0"],
                             deleteExistingData,
                             blockUntilRunning)
  }

  /**
   * Create a full cluster with a master & the requested no. of region servers
   * @param clustername cluster name
   * @param size # of nodes
   * @param extraArgs list of extra args to add to the creation command
   * @param deleteExistingData should the data of any existing cluster
   * of this name be deleted
   * @param blockUntilRunning block until the AM is running
   * @return launcher which will have executed the command.
   */
  public ServiceLauncher createHoyaCluster(String clustername, int size, List<String> extraArgs, boolean deleteExistingData, boolean blockUntilRunning) {
    assert clustername != null
    assert miniCluster != null
    if (deleteExistingData) {
      HadoopFS dfs = HadoopFS.get(new URI(fsDefaultName), miniCluster.config)
      Path clusterDir = HoyaUtils.buildHoyaClusterDirPath(dfs, clustername)
      log.info("deleting customer data at $clusterDir")
      //this is a safety check to stop us doing something stupid like deleting /
      assert clusterDir.toString().contains("/.hoya/")
      dfs.delete(clusterDir, true)
    }

    List<String> argsList = [
        HoyaActions.ACTION_CREATE, clustername,
        CommonArgs.ARG_WORKERS, Integer.toString(size),
        ClientArgs.ARG_MANAGER, RMAddr,
        CommonArgs.ARG_ZKHOSTS, ZKHosts,
        CommonArgs.ARG_ZKPORT, ZKPort.toString(),
        ClientArgs.ARG_WAIT, WAIT_TIME_ARG,
        ClientArgs.ARG_FILESYSTEM, fsDefaultName,
        CommonArgs.ARG_OPTION, OptionKeys.OPTION_TEST, "true",
        CommonArgs.ARG_CONFDIR, getConfDir()
    ]
    
    argsList += HBaseImageCommands
    
    if (extraArgs != null) {
      argsList += extraArgs;
    }
    ServiceLauncher launcher = launchHoyaClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        //varargs list of command line params
        argsList
    )
    assert launcher.serviceExitCode == 0
    HoyaClient hoyaClient = (HoyaClient) launcher.service
    if (blockUntilRunning) {
      hoyaClient.monitorAppToRunning(new Duration(CLUSTER_GO_LIVE_TIME))
    }
    return launcher;
  }

  public String getConfDir() {
    return getResourceConfDirURI()
  }

  /**
   * Start a cluster that has already been defined
   * @param clustername cluster name
   * @param extraArgs list of extra args to add to the creation command
   * @param blockUntilRunning block until the AM is running
   * @return launcher which will have executed the command.
   */
  public ServiceLauncher thawHoyaCluster(String clustername, List<String> extraArgs, boolean blockUntilRunning) {
    assert clustername != null
    assert miniCluster != null

    List<String> argsList = [
        HoyaActions.ACTION_THAW, clustername,
        ClientArgs.ARG_MANAGER, RMAddr,
        ClientArgs.ARG_WAIT, WAIT_TIME_ARG,
        ClientArgs.ARG_FILESYSTEM, fsDefaultName,
    ]
    if (extraArgs != null) {
      argsList += extraArgs;
    }
    ServiceLauncher launcher = launchHoyaClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        //varargs list of command line params
        argsList
    )
    assert launcher.serviceExitCode == 0
    HoyaClient hoyaClient = (HoyaClient) launcher.service
    if (blockUntilRunning) {
      hoyaClient.monitorAppToRunning(new Duration(CLUSTER_GO_LIVE_TIME))
    }
    return launcher;
  }

  
  /**
   * Get the resource configuration dir in the source tree
   * @return
   */
  public File getResourceConfDir() {
    File f = new File("src/main/resources/conf").absoluteFile
    assert f.exists()
    return f
  }

  /**
   get a URI string to the resource conf dir that is suitable for passing down
   to the AM -and works even when the default FS is hdfs
   */
  public String getResourceConfDirURI() {
    return resourceConfDir.absoluteFile.toURI().toString()
  }


  public void logReport(ApplicationReport report) {
    log.info(HoyaUtils.reportToString(report))
  }


  public void logApplications(List<ApplicationReport> apps) {
    apps.each { ApplicationReport r -> logReport(r) }
  }

  /**
   * Wait for the cluster live; fail if it isn't within the (standard) timeout
   * @param hoyaClient client
   * @return the app report of the live cluster
   */
  public ApplicationReport waitForClusterLive(HoyaClient hoyaClient) {
    ApplicationReport report = hoyaClient.monitorAppToRunning(
        new Duration(CLUSTER_GO_LIVE_TIME))
    assertNotNull("Cluster did not go live in the time $CLUSTER_GO_LIVE_TIME", report);
    return report
  }

  /**
   * force kill the application after waiting {@link #WAIT_TIME} for
   * it to shut down cleanly
   * @param hoyaClient client to talk to
   */
  public void waitForAppToFinish(HoyaClient hoyaClient) {
    if (!hoyaClient.monitorAppToCompletion(new Duration(WAIT_TIME))) {
      log.info("Forcibly killing application")
      hoyaClient.forceKillApplication("timed out waiting for application to complete");
    }
  }

  public void assertHBaseMasterNotStopped(HoyaClient hoyaClient,
                                          String clustername) {
    String[] nodes = hoyaClient.listNodesByRole(HBaseKeys.ROLE_MASTER);
    int masterNodeCount = nodes.length;
    assert masterNodeCount> 0;
    ClusterNode node = hoyaClient.getNode(nodes[0]);
    if (node.state >= ClusterDescription.STATE_STOPPED) {
      //stopped, not what is wanted
      log.error("HBase master has stopped")
      log.error(node.toString())
      fail("HBase master has stopped " + node.diagnostics)
    }
  }

  /**
   * Create an HBase config to work with
   * @param hoyaClient hoya client
   * @param clustername cluster
   * @return an hbase config extended with the custom properties from the
   * cluster, including the binding to the HBase cluster
   */
  public Configuration createHBaseConfiguration(HoyaClient hoyaClient) {
    Configuration siteConf = fetchHBaseClientSiteConfig(hoyaClient)
    Configuration conf = HBaseConfiguration.create(siteConf);
/*
    
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 0);
    conf.setInt("zookeeper.recovery.retry", 0)

*/
    return conf
  }

  /**
   * Fetch the current hbase site config from the Hoya AM, from the 
   * <code>hBaseClientProperties</code> field of the ClusterDescription
   * @param hoyaClient client
   * @param clustername name of the cluster
   * @return the site config
   */
  public Configuration fetchHBaseClientSiteConfig(HoyaClient hoyaClient) {
    ClusterDescription status = hoyaClient.getClusterStatus();
    Configuration siteConf = new Configuration(false)
    status.clientProperties.each { String key, String val ->
      siteConf.set(key, val, "hoya cluster");
    }
    return siteConf
  }

  /**
   * Create an (unshared) HConnection talking to the hbase service that
   * Hoya should be running
   * @param hoyaClient hoya client
   * @param clustername the name of the Hoya cluster
   * @return the connection
   */
  public HConnection createHConnection(HoyaClient hoyaClient) {
    Configuration clientConf = createHBaseConfiguration(hoyaClient)
    HConnection hbaseConnection = HConnectionManager.createConnection(clientConf)
    return hbaseConnection;
  }

  public ExecutorService createExecutorService() {
    ThreadPoolExecutor pool = new ThreadPoolExecutor(1, 1,
                                                     1000L,
                                                     TimeUnit.SECONDS,
                                                     new SynchronousQueue<Runnable>());
    pool.allowCoreThreadTimeOut(true);
    return pool;
  }

  /**
   * get a string representation of an HBase cluster status
   * @param status cluster status
   * @return a summary for printing
   */
  String statusToString(ClusterStatus status) {
    StringBuilder builder = new StringBuilder();
    builder << "Cluster " << status.clusterId
    builder << " @ " << status.master << " version " << status.HBaseVersion
    builder << "\nlive [\n"
    if (!status.servers.empty) {
      status.servers.each() { ServerName name ->
        builder << " Server "<< name << " :" << status.getLoad(name) << "\n"
      }
    } else {
    }
    builder << "]\n"
    if (status.deadServers > 0) {
      builder << "\n dead servers=${status.deadServers}"
    }
    return builder.toString()
  }

  public ClusterStatus getHBaseClusterStatus(HoyaClient hoyaClient) {
    try {
      HConnection hbaseConnection = createHConnection(hoyaClient)
      HBaseAdmin hBaseAdmin = new HBaseAdmin(hbaseConnection)
      ClusterStatus hBaseClusterStatus = hBaseAdmin.clusterStatus
      return hBaseClusterStatus
    } catch (NoSuchMethodError e) {
      throw new Exception("Using an incompatible version of HBase!", e)
    }
    
  }

  /**
   * Wait for the hbase master to be live (or past it in the lifecycle)
   * @param clustername cluster
   * @param spintime time to wait
   * @return true if the cluster came out of the sleep time live 
   * @throws IOException
   * @throws HoyaException
   */
  public boolean spinForClusterStartup(HoyaClient hoyaClient, long spintime)
  throws WaitTimeoutException, IOException, HoyaException {
    int state = hoyaClient.waitForRoleInstanceLive(HBaseKeys.MASTER, spintime);
    return state == ClusterDescription.STATE_LIVE;
  }

  /**
   * Teardown-time cluster termination; will stop the cluster iff the client
   * is not null
   * @param hoyaClient client
   * @param clustername name of cluster to teardown
   * @return
   */
  public int maybeStopCluster(HoyaClient hoyaClient, String clustername) {
    if (hoyaClient != null) {
      if (!clustername) {
        clustername = hoyaClient.deployedClusterName;
      }
      //only stop a cluster that exists
      if (clustername) {
        return clusterActionFreeze(hoyaClient, clustername);
      }  
    }
    return 0;
  }
  
  /**
   * stop the cluster via the stop action -and wait for {@link #HBASE_CLUSTER_STOP_TIME}
   * for the cluster to stop. If it doesn't
   * @param hoyaClient client
   * @param clustername cluster
   * @return the exit code
   */
  public int clusterActionFreeze(HoyaClient hoyaClient, String clustername) {
    log.info("Freezing cluster $clustername")
    int exitCode = hoyaClient.actionFreeze(clustername, HBASE_CLUSTER_STOP_TIME);
    if (exitCode != 0) {
      log.warn("HBase app shutdown failed with error code $exitCode")
    }
    return exitCode
  }

  /**
   * Ask the AM for the site configuration -then dump it
   * @param hoyaClient
   * @param clustername
   */
  public void dumpHBaseClientConf(HoyaClient hoyaClient) {
    Configuration conf = fetchHBaseClientSiteConfig(hoyaClient)
    describe("AM-generated site configuration")
    ConfigHelper.dumpConf(conf)
  }

  /**
   * Create a full HBase configuration by merging the AM data with
   * the rest of the local settings. This is the config that would
   * be used by any clients
   * @param hoyaClient hoya client
   * @param clustername name of the cluster
   */
  public void dumpFullHBaseConf(HoyaClient hoyaClient) {
    Configuration conf = createHBaseConfiguration(hoyaClient)
    describe("HBase site configuration from AM")
    ConfigHelper.dumpConf(conf)
  }


  public ClusterStatus basicHBaseClusterStartupSequence(HoyaClient hoyaClient) {
    int hbaseState = hoyaClient.waitForRoleInstanceLive(HBaseKeys.ROLE_MASTER,
                                                        HBASE_CLUSTER_STARTUP_TIME);
    assert hbaseState == ClusterDescription.STATE_LIVE
    //sleep for a bit to give things a chance to go live
    assert spinForClusterStartup(hoyaClient, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)

    //grab the conf from the status and verify the ZK binding matches

    ClusterStatus clustat = getHBaseClusterStatus(hoyaClient)
    describe("HBASE CLUSTER STATUS \n " + statusToString(clustat));
    return clustat
  }

  /**
   * Spin waiting for the RS count to match expected
   * @param hoyaClient client
   * @param clustername cluster name
   * @param regionServerCount RS count
   * @param timeout timeout
   */
  public ClusterStatus waitForHBaseRegionServerCount(HoyaClient hoyaClient,
                                                     String clustername,
                                                     int regionServerCount,
                                                     int timeout) {
    Duration duration = new Duration(timeout);
    duration.start()
    ClusterStatus clustat = null;
    while (true) {
      clustat = getHBaseClusterStatus(hoyaClient)
      int workerCount = clustat.servers.size()
      if (workerCount == regionServerCount) {
        break;
      }
      if (duration.limitExceeded) {
        describe("Cluster region server count of $regionServerCount not met:")
        log.info(statusToString(clustat))
        ClusterDescription status = hoyaClient.getClusterStatus(clustername);
        fail("Expected $regionServerCount YARN region servers," +
             " but  after $timeout millis saw $workerCount in ${statusToString(clustat)}" +
             " \n ${prettyPrint(status.toJsonString())}");
      }
      log.info("Waiting for $regionServerCount region servers -got $workerCount")
      Thread.sleep(1000)
    }
    return clustat;
  }

  
  /**
   * Spin waiting for the Hoya worker count to match expected
   * @param hoyaClient client
   * @param desiredCount RS count
   * @param timeout timeout
   */
  public ClusterDescription waitForHoyaWorkerCount(HoyaClient hoyaClient,
                                                     int desiredCount,
                                                     int timeout) {
    return waitForRoleCount(hoyaClient, HBaseKeys.ROLE_WORKER, desiredCount, timeout)
  }

  /**
   * Spin waiting for the Hoya role count to match expected
   * @param hoyaClient client
   * @param role role to look for
   * @param desiredCount RS count
   * @param timeout timeout
   */
  public ClusterDescription waitForRoleCount(HoyaClient hoyaClient, String role, int desiredCount, int timeout) {
    String clustername = hoyaClient.deployedClusterName;
    ClusterDescription status = null
    Duration duration = new Duration(timeout);
    duration.start()
    while (true) {
      status = hoyaClient.getClusterStatus(clustername)
      
      Integer instances = status.instances[role];
      int instanceCount = instances != null ? instances.intValue() : 0;
      if (instanceCount == desiredCount) {
        break;
      }

      String[] nodes = hoyaClient.listNodesByRole(role);
      if (duration.limitExceeded) {
        describe("Cluster region server count of $desiredCount not met")
        log.info(prettyPrint(status.toJsonString()))
        fail("Expected $desiredCount nodes in role $role," +
             " but saw $instanceCount instances after $timeout millis [$nodes] ")
      }
      log.info("Waiting for $desiredCount workers -got $instanceCount and nodes $nodes")
      Thread.sleep(1000)
    }
    return status
  }

  String prettyPrint(String json) {
    JsonOutput.prettyPrint(json)
  }
  
  void dumpClusterDescription(String text, ClusterDescription status) {
    describe(text)
    log.info(prettyPrint(status.toJsonString()))
  }
  
  void assertExceptionDetails(ServiceLaunchException ex, int exitCode, String text){
    assert exitCode == ex.exitCode
    if (text) {
      assert ex.toString().contains(text)
    }
  }


  public boolean flexClusterTestRun(String clustername, int workers, int flexTarget, boolean persist, boolean testHBaseAfter) {
    createMiniCluster(clustername, createConfiguration(),
                      1,
                      true)
    //now launch the cluster
    HoyaClient hoyaClient = null
    ServiceLauncher launcher = createHoyaCluster(clustername, workers, [], true, true)
    hoyaClient = (HoyaClient) launcher.service
    try {
      basicHBaseClusterStartupSequence(hoyaClient)

      describe("Waiting for initial worker count of $workers")

      //verify the #of region servers is as expected
      //get the hbase status
      waitForHoyaWorkerCount(hoyaClient, workers, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)
      log.info("Hoya worker count at $workers, waiting for region servers to match")
      waitForHBaseRegionServerCount(hoyaClient, clustername, workers, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)

      //start to add some more workers
      describe("Flexing from $workers worker(s) to $flexTarget worker")
      boolean flexed
      flexed = 0 == hoyaClient.flex(clustername, flexTarget, 0, persist)
      waitForHoyaWorkerCount(hoyaClient,flexTarget, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)
      if (testHBaseAfter) {
        waitForHBaseRegionServerCount(hoyaClient, clustername, flexTarget, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)
      }
      return flexed
    } finally {
      maybeStopCluster(hoyaClient,"")
    }


  }
  

}
