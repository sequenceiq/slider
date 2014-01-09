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

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem as HadoopFS
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.hoya.HoyaExitCodes
import org.apache.hadoop.hoya.HoyaXMLConfKeysForTesting
import org.apache.hadoop.hoya.api.ClusterDescription
import org.apache.hadoop.hoya.api.ClusterNode
import org.apache.hadoop.hoya.api.OptionKeys
import org.apache.hadoop.hoya.api.RoleKeys
import org.apache.hadoop.hoya.exceptions.ErrorStrings
import org.apache.hadoop.hoya.exceptions.HoyaException
import org.apache.hadoop.hoya.exceptions.WaitTimeoutException
import org.apache.hadoop.hoya.providers.hbase.HBaseConfigFileOptions
import org.apache.hadoop.hoya.providers.hbase.HBaseKeys
import org.apache.hadoop.hoya.tools.BlockingZKWatcher
import org.apache.hadoop.hoya.tools.Duration
import org.apache.hadoop.hoya.tools.HoyaUtils
import org.apache.hadoop.hoya.yarn.Arguments
import org.apache.hadoop.hoya.yarn.HoyaActions
import org.apache.hadoop.hoya.yarn.KeysForTests
import org.apache.hadoop.hoya.yarn.MicroZKCluster
import org.apache.hadoop.hoya.tools.ZKIntegration
import org.apache.hadoop.hoya.yarn.appmaster.HoyaAppMaster
import org.apache.hadoop.hoya.yarn.client.HoyaClient
import org.apache.hadoop.hoya.yarn.params.ActionFreezeArgs
import org.apache.hadoop.service.ServiceOperations
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.YarnApplicationState
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
public abstract class YarnMiniClusterTestBase extends ServiceLauncherBaseTest
implements KeysForTests, HoyaExitCodes, HoyaXMLConfKeysForTesting {

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
  public static final String HMASTER = "HMaster"

  public static final int SIGTERM = -15
  public static final int SIGKILL = -9
  public static final int SIGSTOP = -19
  public static final String SERVICE_LAUNCHER = "ServiceLauncher"
  public static
  final String NO_ARCHIVE_DEFINED = "Archive configuration option not set: "
  public static final String YRAM = "256"

  protected MiniDFSCluster hdfsCluster
  protected MiniYARNCluster miniCluster;
  protected MicroZKCluster microZKCluster
  protected boolean switchToImageDeploy = false
  protected boolean imageIsRemote = false
  protected URI remoteImageURI

  protected List<HoyaClient> clustersToTeardown = [];


  @Rule
  public final Timeout testTimeout = new Timeout(10*60*1000);


  @After
  public void teardown() {
    describe("teardown")
    stopRunningClusters();
    stopMiniCluster();
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
        maybeStopCluster(hoyaClient, "", "teardown");
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


  public ZKIntegration createZKIntegrationInstance(String zkQuorum, String clusterName, boolean createClusterPath, boolean canBeReadOnly, int timeout) {
    
    BlockingZKWatcher watcher = new BlockingZKWatcher();
    ZKIntegration zki = ZKIntegration.newInstance(zkQuorum,
                                                  USERNAME,
                                                  clusterName,
                                                  createClusterPath,
                                                  canBeReadOnly, watcher) 
    zki.init()
    //here the callback may or may not have occurred.
    //optionally wait for it
    if (timeout > 0) {
      watcher.waitForZKConnection(timeout)
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
      createMiniHDFSCluster(name, conf)
    }

  }

  /**
   * Create a mini HDFS cluster
   * @param name
   * @param conf
   */
  public void createMiniHDFSCluster(String name, YarnConfiguration conf) {
    File baseDir = new File("./target/hdfs/$name").absoluteFile;
    //use file: to rm it recursively
    FileUtil.fullyDelete(baseDir)
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.absolutePath)
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf)
    hdfsCluster = builder.build()
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
    ServiceLauncher launcher = launchHoyaClientNoExitCodeCheck(conf, args)
    int exited = launcher.serviceExitCode
    if (exited != 0) {
      throw new HoyaException(exited,"Launch failed with exit code $exited")
    }
    return launcher;
  }

  /**
   * Launch the hoya client with the specific args against the MiniMR cluster
   * without any checks for exit codes
   * @param conf configuration
   * @param args arg list
   * @return the return code
   */
  public ServiceLauncher launchHoyaClientNoExitCodeCheck(
      Configuration conf,
      List args) {
    assert miniCluster != null
    ResourceManager rm = miniCluster.resourceManager
    log.info("Connecting to rm at ${rm}")

    if (!args.contains(Arguments.ARG_MANAGER)) {
      args += [Arguments.ARG_MANAGER, RMAddr]
    }
    ServiceLauncher launcher = execHoyaCommand(conf, args)
    return launcher
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
   * Kill all Hoya Services. That i
   * @param signal
   */
  public void killHoyaAM(int signal) {
    killJavaProcesses(HoyaAppMaster.SERVICE_CLASSNAME, signal)
  }
  
  /**
   * Kill any java process with the given grep pattern
   * @param grepString string to grep for
   */
  public void killJavaProcesses(String grepString, int signal) {

    GString bashCommand = "jps -l| grep ${grepString} | awk '{print \$1}' | xargs kill $signal"
    log.info("Bash command = $bashCommand" )
    Process bash = ["bash", "-c", bashCommand].execute()
    bash.waitFor()
    
    log.info(bash.in.text)
    log.error(bash.err.text)
  }

  public void killJavaProcesses(List<String> greps, int signal) {
    for (String grep : greps) {
      killJavaProcesses(grep,signal)
    }
  }

  /**
   * List any java process with the given grep pattern
   * @param grepString string to grep for
   */
  public String lsJavaProcesses() {
    Process bash = ["jps","-v"].execute()
    bash.waitFor()
    String out = bash.in.text
    log.info(out)
    String err = bash.err.text
    log.error(err)
    return out + "\n" + err
  }


  public void killServiceLaunchers(int value) {
    killHoyaAM(value)
  }

  public YarnConfiguration getTestConfiguration() {
    YarnConfiguration conf = createConfiguration()

    conf.addResource(HOYA_TEST)
    return conf
  }

  public void assumeConfOptionSet(Configuration conf, String key) {
    Assume.assumeNotNull("npt defined " + key, conf.get(key))
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
  
  protected int getWaitTimeMillis(Configuration conf) {
    
    return WAIT_TIME * 1000;
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
    Map<String, Integer> roles = [
        (HBaseKeys.ROLE_MASTER): 0,
        (HBaseKeys.ROLE_WORKER): size,
    ];
    return createHoyaCluster(clustername,
                             roles,
                             [],
                             deleteExistingData,
                             blockUntilRunning,
                             [:])
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
  public ServiceLauncher createHBaseCluster(String clustername, int size, List<String> extraArgs, boolean deleteExistingData, boolean blockUntilRunning) {
    Map<String, Integer> roles = [
        (HBaseKeys.ROLE_MASTER): 1,
        (HBaseKeys.ROLE_WORKER): size,
    ];
    extraArgs << Arguments.ARG_ROLEOPT << HBaseKeys.ROLE_MASTER << RoleKeys.YARN_MEMORY << YRAM
    extraArgs << Arguments.ARG_ROLEOPT << HBaseKeys.ROLE_WORKER << RoleKeys.YARN_MEMORY << YRAM
    return createHoyaCluster(clustername,
                             roles,
                             extraArgs,
                             deleteExistingData,
                             blockUntilRunning,
                             [:])

  }

  /**
   * Create a hoya cluster
   * @param clustername cluster name
   * @param roles map of rolename to count
   * @param extraArgs list of extra args to add to the creation command
   * @param deleteExistingData should the data of any existing cluster
   * of this name be deleted
   * @param blockUntilRunning block until the AM is running
   * @param clusterOps map of key=value cluster options to set with the --option arg
   * @return launcher which will have executed the command.
   */
  public ServiceLauncher createHoyaCluster(String clustername, Map<String, Integer> roles, List<String> extraArgs, boolean deleteExistingData, boolean blockUntilRunning, Map<String, String> clusterOps) {
    createOrBuildHoyaCluster(HoyaActions.ACTION_CREATE,clustername,roles,extraArgs,deleteExistingData,blockUntilRunning,clusterOps)
  }

  /**
   * Create or build a hoya cluster (the action is set by the first verb)
   * @param action operation to invoke: HoyaActions.ACTION_CREATE or HoyaActions.ACTION_BUILD
   * @param clustername cluster name
   * @param roles map of rolename to count
   * @param extraArgs list of extra args to add to the creation command
   * @param deleteExistingData should the data of any existing cluster
   * of this name be deleted
   * @param blockUntilRunning block until the AM is running
   * @param clusterOps map of key=value cluster options to set with the --option arg
   * @return launcher which will have executed the command.
   */
  public ServiceLauncher  createOrBuildHoyaCluster(String action, String clustername, Map<String, Integer> roles, List<String> extraArgs, boolean deleteExistingData, boolean blockUntilRunning, Map<String, String> clusterOps) {
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


    List<String> roleList = [];
    roles.each { String role, Integer val -> 
      log.info("Role $role := $val")
      roleList << Arguments.ARG_ROLE << role << Integer.toString(val)
    }
    
    List<String> argsList = [
        action, clustername,
        Arguments.ARG_MANAGER, RMAddr,
        Arguments.ARG_ZKHOSTS, ZKHosts,
        Arguments.ARG_ZKPORT, ZKPort.toString(),
        Arguments.ARG_FILESYSTEM, fsDefaultName,
        Arguments.ARG_OPTION, OptionKeys.HOYA_TEST_FLAG, "true",
        Arguments.ARG_DEBUG,
        Arguments.ARG_CONFDIR, confDir
    ]
    if (blockUntilRunning) {
      argsList << Arguments.ARG_WAIT << WAIT_TIME_ARG
    }

    argsList += roleList;
    argsList += imageCommands

    //now inject any cluster options
    clusterOps.each { String opt, String val ->
      argsList << Arguments.ARG_OPTION << opt << val;
    }
    
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
    return resourceConfDirURI
  }

  /**
   * Get the key for the application
   * @return
   */
  public String getApplicationHomeKey() {
    return KEY_HOYA_TEST_HBASE_HOME
  }
  /**
   * Get the archive path -which defaults to the local one
   * @return
   */
  public String getArchivePath() {
    return localArchive
  }
  /**
   * Get the local archive -the one defined in the test configuration
   * @return a possibly null/empty string
   */
  public final String getLocalArchive() {
    return testConfiguration.getTrimmed(archiveKey)
  }
  /**
   * Get the key for archives in tests
   * @return
   */
  public String getArchiveKey() {
    return KEY_HOYA_TEST_HBASE_TAR
  }

  public void assumeArchiveDefined() {
    String archive = archivePath
    boolean defined = archive != null && archive != ""
    if (!defined) {
      log.warn(YarnMiniClusterTestBase.NO_ARCHIVE_DEFINED + archiveKey);
    }
    Assume.assumeTrue(NO_ARCHIVE_DEFINED + archiveKey, defined)
  }
  /**
   * Assume that HBase home is defined. This does not check that the
   * path is valid -that is expected to be a failure on tests that require
   * HBase home to be set.
   */
  public void assumeApplicationHome() {
    Assume.assumeTrue("Application home dir option not set " + applicationHomeKey,
                      applicationHome != null && applicationHome != "")
  }


  public String getApplicationHome() {
    return testConfiguration.getTrimmed(applicationHomeKey)
  }

  public List<String> getImageCommands() {
    if (switchToImageDeploy) {
      // its an image that had better be defined
      assert archivePath
      if (!imageIsRemote) {
        // its not remote, so assert it exists
        File f = new File(archivePath)
        assert f.exists()
        return [Arguments.ARG_IMAGE, f.toURI().toString()]
      } else {
        assert remoteImageURI

        // if it is remote, then its whatever the archivePath property refers to
        return [Arguments.ARG_IMAGE, remoteImageURI.toString()];
      }
    } else {
      assert applicationHome
      assert new File(applicationHome).exists();
      return [Arguments.ARG_APP_HOME, applicationHome]
    }
  }

  public void not_implemented() {
    fail("Not implemented")
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
        Arguments.ARG_MANAGER, RMAddr,
        Arguments.ARG_WAIT, WAIT_TIME_ARG,
        Arguments.ARG_FILESYSTEM, fsDefaultName,
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
    File f = new File(testConfigurationPath).absoluteFile;
    if (!f.exists()) {
      throw new FileNotFoundException("Resource configuration directory $f not found")
    }
    return f;
  }

  public String getTestConfigurationPath() {
    fail("Not implemented");
    null;
  }

  /**
   get a URI string to the resource conf dir that is suitable for passing down
   to the AM -and works even when the default FS is hdfs
   */
  public String getResourceConfDirURI() {;
    return resourceConfDir.absoluteFile.toURI().toString()
  }


  public void logReport(ApplicationReport report) {
    log.info(HoyaUtils.reportToString(report));
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
        new Duration(CLUSTER_GO_LIVE_TIME));
    assertNotNull("Cluster did not go live in the time $CLUSTER_GO_LIVE_TIME", report);
    return report;
  }

  /**
   * force kill the application after waiting {@link #WAIT_TIME} for
   * it to shut down cleanly
   * @param hoyaClient client to talk to
   */
  public ApplicationReport waitForAppToFinish(HoyaClient hoyaClient) {

    int waitTime = getWaitTimeMillis(hoyaClient.config)
    return waitForAppToFinish(hoyaClient, waitTime)
  }

  public ApplicationReport waitForAppToFinish(
      HoyaClient hoyaClient,
      int waitTime) {
    ApplicationReport report = hoyaClient.monitorAppToState(new Duration(
        waitTime), YarnApplicationState.FINISHED);
    if (report == null) {
      log.info("Forcibly killing application")
      dumpClusterStatus(hoyaClient, "final application status")
      //list all the nodes' details
      List<ClusterNode> nodes = listNodesInRole(hoyaClient, "")
      if (nodes.empty) {
        log.info("No live nodes")
      }
      nodes.each { ClusterNode node -> log.info(node.toString())}
      hoyaClient.forceKillApplication("timed out waiting for application to complete");
    }
    return report;
  }

  public void dumpClusterStatus(HoyaClient hoyaClient, String text) {
    ClusterDescription status = hoyaClient.getClusterDescription();
    dumpClusterDescription(text, status)
  }

  List<ClusterNode> listNodesInRole(HoyaClient hoyaClient, String role) {
    return hoyaClient.listClusterNodesInRole(role)
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
  public int maybeStopCluster(HoyaClient hoyaClient, String clustername, String message) {
    if (hoyaClient != null) {
      if (!clustername) {
        clustername = hoyaClient.deployedClusterName;
      }
      //only stop a cluster that exists
      if (clustername) {
        return clusterActionFreeze(hoyaClient, clustername, message);
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
  public int clusterActionFreeze(HoyaClient hoyaClient, String clustername, String message = "action freeze") {
    log.info("Freezing cluster $clustername")
    ActionFreezeArgs freezeArgs  = new ActionFreezeArgs();
    freezeArgs.waittime = HBASE_CLUSTER_STOP_TIME
    int exitCode = hoyaClient.actionFreeze(clustername,
                                           message,
                                           freezeArgs);
    if (exitCode != 0) {
      log.warn("Cluster freeze failed with error code $exitCode")
    }
    return exitCode
  }


  public void waitWhileClusterExists(HoyaClient client, int timeout) {
    Duration duration = new Duration(timeout);
    duration.start()
    while(client.actionExists(client.deployedClusterName) && !duration.limitExceeded) {
      sleep(1000);
    }
  }

  /**
   * Spin waiting for the Hoya role count to match expected
   * @param hoyaClient client
   * @param role role to look for
   * @param desiredCount RS count
   * @param timeout timeout
   */
  public ClusterDescription waitForRoleCount(HoyaClient hoyaClient, String role, int desiredCount, int timeout) {
    return waitForRoleCount(hoyaClient, [(role): desiredCount], timeout)
  }
  
  /**
   * Spin waiting for the Hoya role count to match expected
   * @param hoyaClient client
   * @param roles map of roles to look for
   * @param desiredCount RS count
   * @param timeout timeout
   */
  public ClusterDescription waitForRoleCount(
      HoyaClient hoyaClient,
      Map<String, Integer> roles,
      int timeout,
      String operation = "startup") {
    String clustername = hoyaClient.deployedClusterName;
    ClusterDescription status = null
    Duration duration = new Duration(timeout);
    duration.start()
    boolean roleCountFound = false;
    while (!roleCountFound) {
      StringBuilder details = new StringBuilder()
      roleCountFound = true;
      status = hoyaClient.getClusterDescription(clustername)

      for (Map.Entry<String, Integer> entry : roles.entrySet()) {
        String role = entry.key
        int desiredCount = entry.value
        Integer instances = status.instances[role];
        int instanceCount = instances != null ? instances.intValue() : 0;
        if (instanceCount != desiredCount) {
          roleCountFound = false;
        }
        details.append("[$role]: $instanceCount of $desiredCount; ")
      }
      if (roleCountFound) {
        //successful
        log.info("$operation: role count as desired: $details")

        break;
      }

      if (duration.limitExceeded) {
        duration.finish();
        describe("$operation: role count not met after $duration: $details")
        log.info(prettyPrint(status.toJsonString()))
        fail("$operation: role counts not met  after $duration: $details in \n$status ")
      }
      log.info("Waiting: " + details)
      Thread.sleep(1000)
    }
    return status
  }


  void dumpClusterDescription(String text, ClusterDescription status) {
    describe(text)
    log.info(prettyPrint(status.toJsonString()))
  }
  
  void assertExceptionDetails(ServiceLaunchException ex, int exitCode, String text = ""){
    if (exitCode != ex.exitCode) {
      log.warn("Wrong exit code, expected $exitCode but got $ex.exitCode in $ex",
               ex)
      assert exitCode == ex.exitCode
    }
    if (text) {
      if (!(ex.toString().contains(text))) {
        log.warn("String match failed in $ex", ex)
        assert ex.toString().contains(text);
      }
      
    }
  }


  String roleMapToString(Map<String,Integer> roles) {
    StringBuilder builder = new StringBuilder()
    roles.each { String name, int value ->
      builder.append("$name->$value ")
    }
    return builder.toString()
  }

  /**
   * Turn on test runs against a copy of the archive that is
   * uploaded to HDFS -this method copies up the
   * archive then switches the tests into archive mode
   */
  public void enableTestRunAgainstUploadedArchive() {
    Path remotePath = copyLocalArchiveToHDFS(localArchive)
    // image mode
    switchToRemoteImageDeploy(remotePath);
  }

  /**
   * Switch to deploying a remote image
   * @param remotePath the remote path to use
   */
  public void switchToRemoteImageDeploy(Path remotePath) {
    switchToImageDeploy = true
    imageIsRemote = true
    remoteImageURI = remotePath.toUri()
  }

  /**
   * Copy a local archive to HDFS
   * @param localArchive local archive
   * @return the path of the uploaded image
   */
  public Path copyLocalArchiveToHDFS(String localArchive) {
    assert localArchive
    File localArchiveFile = new File(localArchive)
    assert localArchiveFile.exists()
    assert hdfsCluster
    Path remoteUnresolvedArchive = new Path(localArchiveFile.name)
    assert FileUtil.copy(
        localArchiveFile,
        hdfsCluster.fileSystem,
        remoteUnresolvedArchive,
        false,
        testConfiguration)
    Path remotePath = hdfsCluster.fileSystem.resolvePath(
        remoteUnresolvedArchive)
    return remotePath
  }

  /**
   * Assert that an operation failed because a cluster is in use
   * @param e exception
   */
  public void assertFailureClusterInUse(HoyaException e) {
    assertExceptionDetails(e,
                           HoyaExitCodes.EXIT_CLUSTER_IN_USE,
                           ErrorStrings.E_CLUSTER_RUNNING)
  }

  /**
   * Assert that a cluster is unknown by the exit code
   * & the {@link HoyaClient#E_UNKNOWN_CLUSTER} text
   * @param exception
   */
  public void assertUnknownClusterException(HoyaException e) {
    assertExceptionDetails(e, HoyaExitCodes.EXIT_UNKNOWN_HOYA_CLUSTER,
                           ErrorStrings.E_UNKNOWN_CLUSTER)
  }

  public void assertSucceeded(ServiceLauncher service) {
    assert 0 == service.serviceExitCode;
  }
}
