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
import groovy.util.logging.Commons
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.hbase.ClusterStatus
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.ServerName
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HConnection
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.hoya.api.ClusterDescription
import org.apache.hadoop.hoya.exceptions.HoyaException
import org.apache.hadoop.hoya.tools.ConfigHelper
import org.apache.hadoop.hoya.tools.Duration
import org.apache.hadoop.hoya.tools.YarnUtils
import org.apache.hadoop.hoya.yarn.CommonArgs
import org.apache.hadoop.hoya.yarn.KeysForTests
import org.apache.hadoop.hoya.yarn.MicroZKCluster
import org.apache.hadoop.hoya.yarn.ZKIntegration
import org.apache.hadoop.hoya.yarn.appmaster.EnvMappings
import org.apache.hadoop.hoya.yarn.client.ClientArgs
import org.apache.hadoop.hoya.yarn.client.HoyaClient
import org.apache.hadoop.service.ServiceOperations
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.MiniYARNCluster
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.apache.hadoop.yarn.service.launcher.ServiceLauncherBaseTest
import org.junit.After
import org.junit.Before
import org.junit.internal.AssumptionViolatedException

import java.util.concurrent.ExecutorService
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Base class for mini cluster tests -creates a field for the
 * mini yarn cluster
 */
@Commons
@CompileStatic

public class YarnMiniClusterTestBase extends ServiceLauncherBaseTest
implements KeysForTests {

  /**
   * Mini YARN cluster only
   */
  public static final int CLUSTER_GO_LIVE_TIME = 1 * 60 * 1000
  public static final int HBASE_CLUSTER_STARTUP_TIME = 2 * 60 * 1000
  public static final int HBASE_CLUSTER_STOP_TIME = 1 * 60 * 1000
  
  /**
   * The time to sleep before trying to talk to the HBase Master and
   * expect meaningful results.
   */
  public static final int HBASE_CLUSTER_STARTUP_TO_LIVE_TIME = 20000
  
  protected MiniDFSCluster hdfsCluster
  protected MiniYARNCluster miniCluster;
  protected MicroZKCluster microZKCluster

  @Before
  public void setup() {

  }

  @After
  public void teardown() {
    describe("teardown")
    stopMiniCluster()
  }

  public void stopMiniCluster() {
    ServiceOperations.stopQuietly(log, miniCluster)
    microZKCluster?.close();
    hdfsCluster?.shutdown();
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
  protected void createMiniCluster(String name, YarnConfiguration conf, int noOfNodeManagers, int numLocalDirs, int numLogDirs, boolean startZK, boolean startHDFS) {
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
    conf.setClass(YarnConfiguration.RM_SCHEDULER,
                  FifoScheduler.class, ResourceScheduler.class);
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
    ResourceManager rm = miniCluster.resourceManager
    log.info("Connecting to rm at ${rm}")

    if (!args.contains(ClientArgs.ARG_MANAGER)) {
      args += [ClientArgs.ARG_MANAGER, RMAddr]
    }
    ServiceLauncher launcher = launch(HoyaClient, conf, args)
    assert launcher.serviceExitCode == 0
    return launcher;
  }


  public String getHBaseHome() {
    YarnConfiguration conf = getTestConfiguration()
    String hbaseHome = conf.getTrimmed(HOYA_TEST_HBASE_HOME)
    return hbaseHome
  }

  public YarnConfiguration getTestConfiguration() {
    YarnConfiguration conf = new YarnConfiguration()

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
    return microZKCluster ? microZKCluster.port : EnvMappings.HBASE_ZK_PORT
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

  /**
   * Create an AM without a master
   * @param clustername AM name
   * @param size # of nodes
   * @param blockUntilRunning block until the AM is running
   * @return launcher which will have executed the command.
   */
  public ServiceLauncher createMasterlessAM(String clustername,
                                            int size,
                                            boolean blockUntilRunning) {
    return createHoyaCluster(clustername, size,
                             [CommonArgs.ARG_X_NO_MASTER],
                             blockUntilRunning)
  }

  public File getResourceConfDir() {
    File f = new File("src/main/resources/conf").absoluteFile
    assert f.exists()
    return f
  }

  /**
   * Create a full cluster with a master & the requested no. of region servers
   * @param clustername cluster name
   * @param size # of nodes
   * @param extraArgs list of extra args to add to the creation command
   * @param blockUntilRunning block until the AM is running
   * @return launcher which will have executed the command.
   */
  public ServiceLauncher createHoyaCluster(String clustername,
                                           int size,
                                           List<String> extraArgs,
                                           boolean blockUntilRunning) {
    assert clustername != null
    assert miniCluster != null
    List<String> argsList = [
        ClientArgs.ACTION_CREATE, clustername,
        CommonArgs.ARG_MIN, Integer.toString(size),
        CommonArgs.ARG_MAX, Integer.toString(size),
        ClientArgs.ARG_MANAGER, RMAddr,
        CommonArgs.ARG_HBASE_HOME, HBaseHome,
        CommonArgs.ARG_ZKQUORUM, ZKHosts,
        CommonArgs.ARG_ZKPORT, ZKPort.toString(),
        CommonArgs.ARG_HBASE_ZKPATH, "/hbase",
        ClientArgs.ARG_WAIT, WAIT_TIME_ARG,
        ClientArgs.ARG_FILESYSTEM, fsDefaultName,
        CommonArgs.ARG_X_TEST,
        CommonArgs.ARG_CONFDIR, getResourceConfDirURI()
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
   get a URI string to the resource conf dir that is suitable for passing down
   to the AM -and works even when the default FS is hdfs
   */
  public String getResourceConfDirURI() {
    return getResourceConfDir().absoluteFile.toURI().toString()
  }


  public void logReport(ApplicationReport report) {
    log.info(YarnUtils.reportToString(report))
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
      hoyaClient.forceKillApplication();
    }
  }

  public void assertHBaseMasterNotStopped(HoyaClient hoyaClient,
                                          String clustername) {
    ClusterDescription status = hoyaClient.getClusterStatus(clustername);
    ClusterDescription.ClusterNode node = status.masterNodes[0];
    assert node != null;
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
  public Configuration createHBaseConfiguration(HoyaClient hoyaClient,
                                                String clustername) {
    Configuration siteConf = fetchHBaseClientSiteConfig(hoyaClient, clustername)
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
  public Configuration fetchHBaseClientSiteConfig(HoyaClient hoyaClient,
                                                  String clustername) {
    ClusterDescription status = hoyaClient.getClusterStatus(clustername);
    Configuration siteConf = new Configuration(false)
    status.hBaseClientProperties.each { String key, String val ->
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
  public HConnection createHConnection(HoyaClient hoyaClient,
                                       String clustername) {
    Configuration clientConf = createHBaseConfiguration(hoyaClient, clustername)
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
    status.with {
      builder << "Cluster " << clusterId
      builder << " @ " << master << " version " << getHBaseVersion()
      builder << "\n"
      status.servers.each() { ServerName name ->
        builder << name << ":" << getLoad(name) << "\n"
      }
    }
    return builder.toString()
  }


  public ClusterStatus getHBaseClusterStatus(HoyaClient hoyaClient, String clustername) {
    try {
      HConnection hbaseConnection = createHConnection(hoyaClient, clustername)

      HBaseAdmin hBaseAdmin = new HBaseAdmin(hbaseConnection)
      ClusterStatus hBaseClusterStatus = hBaseAdmin.clusterStatus
      return hBaseClusterStatus
    } catch (NoSuchMethodError e) {
      //this looks like some version mismatch: downgrade to a skip
      throw new AssumptionViolatedException("HBase connection version problems", e, null);
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
  public boolean spinForClusterStartup(HoyaClient hoyaClient, String clustername, long spintime)
  throws IOException, HoyaException {
    Duration duration = new Duration(spintime).start();
    boolean live = true;
    while (live && !duration.limitExceeded) {
      ClusterDescription cd = hoyaClient.getClusterStatus(clustername)
      //see if there is a master node yet
      if (cd.masterNodes.size() != 0) {
        //if there is, get the node
        ClusterDescription.ClusterNode master = cd.masterNodes[0];
        live = master.state == ClusterDescription.STATE_LIVE
        if (!live) {
          break
        }
      }
      try {
        Thread.sleep(5000);
      } catch (InterruptedException ignored) {
        //ignored
      }
    }
    return live;
  }

  /**
   * stop the cluster via the stop action 
   * @param hoyaClient client
   * @param clustername cluster
   */
  public void clusterActionStop(HoyaClient hoyaClient, String clustername) {

    hoyaClient.actionStop(clustername);
    int exitCode = hoyaClient.monitorAppToCompletion(
        new Duration(HBASE_CLUSTER_STOP_TIME))
    if (exitCode != 0) {
      log.warn("HBase app shutdown failed with error code $exitCode")
    }
  }

  /**
   * Ask the AM for the site configuration -then dump it
   * @param hoyaClient
   * @param clustername
   */
  public void dumpHBaseClientConf(HoyaClient hoyaClient, String clustername) {
    Configuration conf = fetchHBaseClientSiteConfig(hoyaClient, clustername)
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
  public void dumpFullHBaseConf(HoyaClient hoyaClient, String clustername) {
    Configuration conf = createHBaseConfiguration(hoyaClient, clustername)
    describe("HBase site configuration from AM")
    ConfigHelper.dumpConf(conf)
  }


  public ClusterStatus basicHBaseClusterStartupSequence(HoyaClient hoyaClient, String clustername) {
    int hbaseState = hoyaClient.waitForHBaseMasterLive(clustername,
                                                       HBASE_CLUSTER_STARTUP_TIME);
    assert hbaseState == ClusterDescription.STATE_LIVE
    dumpHBaseClientConf(hoyaClient, clustername)
    //sleep for a bit to give things a chance to go live
    assert spinForClusterStartup(hoyaClient, clustername, HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)

    //grab the conf from the status and verify the ZK binding matches

    ClusterStatus clustat = getHBaseClusterStatus(hoyaClient, clustername)
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
  public ClusterDescription waitForRegionServerCount(HoyaClient hoyaClient, String clustername, int regionServerCount, int timeout) {
    ClusterDescription status = null
    Duration duration = new Duration(timeout);
    duration.start()
    int workerCount = 0;
    while (workerCount == 0) {
      status = hoyaClient.getClusterStatus(clustername)
      //log.info("Status $status")
      workerCount = status.regionNodes.size()
      if (workerCount == 0) {
        if (duration.limitExceeded) {
          describe("Cluster region server count of $regionServerCount not reached")
          log.info(status.toJsonString())
          assert regionServerCount == workerCount;
        }
        Thread.sleep(5000)
      }
    }
    return status
  }

  String prettyPrint(String json) {
    JsonOutput.prettyPrint(json)
  }
  
  String log(String text, ClusterDescription status) {
    describe(text)
    log.info(prettyPrint(status.toJsonString()))
  }
  
  
}
