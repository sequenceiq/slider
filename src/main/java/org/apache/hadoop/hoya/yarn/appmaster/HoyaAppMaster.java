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

package org.apache.hadoop.hoya.yarn.appmaster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hoya.api.OptionKeys;
import org.apache.hadoop.hoya.api.RoleKeys;
import org.apache.hadoop.hoya.exceptions.NoSuchNodeException;
import org.apache.hadoop.hoya.providers.ClusterExecutor;
import org.apache.hadoop.hoya.providers.HoyaProviderFactory;
import org.apache.hadoop.hoya.providers.ProviderRole;
import org.apache.hadoop.hoya.providers.ProviderUtils;
import org.apache.hadoop.hoya.providers.hbase.HBaseConfigFileOptions;
import org.apache.hadoop.hoya.providers.hbase.HBaseKeys;
import org.apache.hadoop.hoya.HoyaExitCodes;
import org.apache.hadoop.hoya.HoyaKeys;
import org.apache.hadoop.hoya.api.ClusterDescription;
import org.apache.hadoop.hoya.api.ClusterNode;
import org.apache.hadoop.hoya.api.HoyaAppMasterProtocol;
import org.apache.hadoop.hoya.exceptions.BadCommandArgumentsException;
import org.apache.hadoop.hoya.exceptions.HoyaException;
import org.apache.hadoop.hoya.exceptions.HoyaInternalStateException;
import org.apache.hadoop.hoya.exec.ApplicationEventHandler;
import org.apache.hadoop.hoya.exec.RunLongLivedApp;
import org.apache.hadoop.hoya.providers.hbase.HBaseProvider;
import org.apache.hadoop.hoya.tools.ConfigHelper;
import org.apache.hadoop.hoya.tools.HoyaUtils;
import org.apache.hadoop.hoya.yarn.HoyaActions;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.service.launcher.RunService;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This is the AM, which directly implements the callbacks from the AM and NM
 * It does not tag interface methods as @Override as the groovyc plugin
 * for maven seems to build in Java 5 syntax, not java6
 */
@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
public class HoyaAppMaster extends CompositeService
  implements AMRMClientAsync.CallbackHandler,
             NMClientAsync.CallbackHandler,
             RunService,
             HoyaExitCodes,
            HBaseKeys,
             HoyaAppMasterProtocol,
             ApplicationEventHandler,
             RoleKeys {
  protected static final Logger log =
    LoggerFactory.getLogger(HoyaAppMaster.class);
  protected static final Logger LOG_AM_PROCESS =
    LoggerFactory.getLogger("org.apache.hadoop.hoya.yarn.appmaster.HoyaAppMaster.master");
  /**
   * How long to expect launcher threads to shut down on AM termination:
   * {@value}
   */
  public static final int LAUNCHER_THREAD_SHUTDOWN_TIME = 10000;
  /**
   * time to wait from shutdown signal being rx'd to telling
   * the AM: {@value}
   */
  public static final int TERMINATION_SIGNAL_PROPAGATION_DELAY = 1000;
  /**
   * Max failures to tolerate for the containers
   */
  public static final int MAX_TOLERABLE_FAILURES = 10;
  public static final String ROLE_UNKNOWN = "unknown";
  public static final int HEARTBEAT_INTERVAL = 1000;
  public static final int DEFAULT_CONTAINER_MEMORY_FOR_WORKER = 10;

  /** YARN RPC to communicate with the Resource Manager or Node Manager */
  private YarnRPC rpc;

  /** Handle to communicate with the Resource Manager*/
  private AMRMClientAsync asyncRMClient;

  /** Handle to communicate with the Node Manager*/
  public NMClientAsync nmClientAsync;

  /** RPC server*/
  private Server server;
  /** Hostname of the container*/
  private String appMasterHostname = "";
  /* Port on which the app master listens for status updates from clients*/
  private int appMasterRpcPort = 0;
  /** Tracking url to which app master publishes info for clients to monitor*/
  private String appMasterTrackingUrl = "";

  /** Application Attempt Id ( combination of attemptId and fail count )*/
  private ApplicationAttemptId appAttemptID;
  // App Master configuration

  /**
   * container memory
   */
  private int containerMemory = DEFAULT_CONTAINER_MEMORY_FOR_WORKER;

  /**
   * Hash map of the containers we have
   */
  private final ConcurrentMap<ContainerId, ContainerInfo> containers =
    new ConcurrentHashMap<ContainerId, ContainerInfo>();

  /**
   * Hash map of the containers we have released, but we
   * are still awaiting acknowledgements on. Any failure of these
   * containers is treated as a successful outcome
   */
  private final ConcurrentMap<ContainerId, Container> containersBeingReleased =
    new ConcurrentHashMap<ContainerId, Container>();

  private final Map<Integer, RoleStatus> roleStatusMap = new HashMap<Integer, RoleStatus>(); 
  
  /**
   *  This is the number of containers which we desire for HoyaAM to maintain
   */
  //private int desiredContainerCount = 0;

  /**
   * Counter for completed containers ( complete denotes successful or failed )
   */
  private final AtomicInteger numCompletedContainers = new AtomicInteger();

  /**
   *   Count of failed containers

   */
  private final AtomicInteger numFailedContainers = new AtomicInteger();

  /**
   * # of started containers
   */
  private final AtomicInteger startedContainers = new AtomicInteger();

  /**
   * # of containers that failed to start 
   */
  private final AtomicInteger startFailedContainers = new AtomicInteger();

  /**
   * Launch threads -these need to unregister themselves after launch,
   * to stop the leakage of threads on many cluster restarts
   */
  private final Map<RoleLauncher, Thread> launchThreads = new HashMap<RoleLauncher, Thread>();
  
  /**
   * Thread group for the launchers; gives them all a useful name
   * in stack dumps
   */
  private final ThreadGroup launcherThreadGroup = new ThreadGroup("launcher");

  /**
   * model the state using locks and conditions
   */
  private final ReentrantLock AMExecutionStateLock = new ReentrantLock();
  private final Condition isAMCompleted = AMExecutionStateLock.newCondition();

  /**
   * Flag set if the AM is to be shutdown
   */
  private final AtomicBoolean amCompletionFlag = new AtomicBoolean(false);
  private volatile boolean localProcessTerminated = false;
  private volatile boolean localProcessStarted = false;
  private volatile boolean success;


  /** Arguments passed in : raw*/
  private HoyaMasterServiceArgs serviceArgs;

  /** Arguments passed in : parsed*/
  private String[] argv;
  
  

  /**
   The cluster description published to callers
   This is used as a synchronization point on activities that update
   the CD, and also to update some of the structures that
   feed in to the CD
   */
  public ClusterDescription clusterSpec = new ClusterDescription();
  /**
   * This is the status, the live model
   */
  public ClusterDescription clusterStatus = new ClusterDescription();

  /**
   * Log for changing cluster descriptions; kept tighter than
   * class synchronization
   */
  public final Object clusterSpecLock = new Object();
  
  
  /**
   * List of completed nodes. This isn't kept in the CD as it gets too
   * big for the RPC responses. Indeed, we should think about how deep to get this
   */
  private final Map<ContainerId, ClusterNode> completedNodes
   = new ConcurrentHashMap<ContainerId, ClusterNode>();

  /**
   * Nodes that failed to start.
   * Again, kept out of the CD
   */
  public final Map<ContainerId, ClusterNode> failedNodes =
    new ConcurrentHashMap<ContainerId, ClusterNode>();

  /**
   * Map of containerID -> cluster nodes, for status reports.
   * Access to this should be synchronized on the clusterDescription
   */
  private final Map<ContainerId, ClusterNode> liveNodes =
    new ConcurrentHashMap<ContainerId, ClusterNode>();

  /**
   * Flag set if there is no master
   */
  private boolean noMaster;

  /**
   * the hbase master runner
   */
  private RunLongLivedApp hbaseMaster;

  /**
   * The master node. This is a shared reference with the clusterDescription;
   * operations on it MUST be synchronised with that object
   */
  private ClusterNode masterNode;

  /**
   * Map of requested nodes. This records the command used to start it,
   * resources, etc. When container started callback is received,
   * the node is promoted from here to the containerMap
   */
  private final Map<ContainerId, ClusterNode> requestedNodes =
    new ConcurrentHashMap<ContainerId, ClusterNode>();

  /**
   * Exit code set when the spawned process exits
   */
  private volatile int spawnedProcessExitCode;
  /**
   * Flag to set if the process exit code was set before shutdown started
   */
  private boolean spawnedProcessExitedBeforeShutdownTriggered;

  /**
   * ID of the AM container
   */
  private ContainerId AppMasterContainerID;
  
  /**
  * Provider of this cluster
   */
  private ClusterExecutor provider;

  /**
   * Service Constructor
   */
  public HoyaAppMaster() {
    super("HoyaMasterService");
  }

/* =================================================================== */
/* service lifecycle methods */
/* =================================================================== */

  @Override //AbstractService
  public synchronized void serviceInit(Configuration conf) throws Exception {
    //sort out the location of the AM
    serviceArgs.applyDefinitions(conf);
    serviceArgs.applyFileSystemURL(conf);

    String rmAddress = serviceArgs.rmAddress;
    if (rmAddress != null) {
      log.debug("Setting rm address from the command line: {}", rmAddress);
      HoyaUtils.setRmSchedulerAddress(conf, rmAddress);
    }
    super.serviceInit(conf);
  }
  
/* =================================================================== */
/* RunService methods called from ServiceLauncher */
/* =================================================================== */

  /**
   * pick up the args from the service launcher
   * @param config
   * @param args argument list
   */
  @Override // RunService
  public Configuration bindArgs(Configuration config, String... args) throws Exception {
    this.argv = args;
    serviceArgs = new HoyaMasterServiceArgs(argv);
    serviceArgs.parse();
    serviceArgs.postProcess();
    return HoyaUtils.patchConfiguration(config);
  }


  /**
   * this is called by service launcher; when it returns the application finishes
   * @return the exit code to return by the app
   * @throws Throwable
   */
  @Override
  public int runService() throws Throwable {

    //choose the action
    String action = serviceArgs.action;
    List<String> actionArgs = serviceArgs.actionArgs;
    int exitCode = EXIT_SUCCESS;
    if (action.equals(HoyaActions.ACTION_HELP)) {
      log.info(getName() + serviceArgs.usage());
    } else if (action.equals(HoyaActions.ACTION_CREATE)) {
      exitCode = createAndRunCluster(actionArgs.get(0));
    } else {
      throw new HoyaException("Unimplemented: " + action);
    }
    return exitCode;
  }

/* =================================================================== */

  /**
   * Create and run the cluster
   * @return exit code
   * @throws Throwable on a failure
   */
  private int createAndRunCluster(String clustername) throws Throwable {

    //load the cluster description from the cd argument
    String hoyaClusterDir = serviceArgs.hoyaClusterURI;
    URI hoyaClusterURI = new URI(hoyaClusterDir);
    Path clusterDirPath = new Path(hoyaClusterURI);
    Path clusterSpecPath =
      new Path(clusterDirPath, HoyaKeys.CLUSTER_SPECIFICATION_FILE);
    FileSystem fs = getClusterFS();
    ClusterDescription.verifyClusterSpecExists(clustername, fs,
                                               clusterSpecPath);

    clusterSpec = ClusterDescription.load(fs, clusterSpecPath);

    
    //get our provider
    HoyaProviderFactory factory =
      HoyaProviderFactory.createHoyaProviderFactory(
        clusterSpec.type);
    provider = factory.createExecutor();

    YarnConfiguration conf = new YarnConfiguration(getConfig());
    InetSocketAddress address = HoyaUtils.getRmSchedulerAddress(conf);
    log.info("RM is at {}", address);
    rpc = YarnRPC.create(conf);

    AppMasterContainerID = ConverterUtils.toContainerId(
      HoyaUtils.mandatoryEnvVariable(
        ApplicationConstants.Environment.CONTAINER_ID.name()));
    appAttemptID = AppMasterContainerID.getApplicationAttemptId();

    ApplicationId appid = appAttemptID.getApplicationId();
    log.info("Hoya AM for ID {}", appid.getId() );


    int heartbeatInterval = HEARTBEAT_INTERVAL;


    //add the RM client -this brings the callbacks in
    asyncRMClient = AMRMClientAsync.createAMRMClientAsync(heartbeatInterval,
                                                          this);
    addService(asyncRMClient);
    //now bring it up
    asyncRMClient.init(conf);
    asyncRMClient.start();


    //nmclient relays callbacks back to this class
    nmClientAsync = new NMClientAsyncImpl("hoya", this);
    addService(nmClientAsync);
    nmClientAsync.init(conf);
    nmClientAsync.start();

    //bring up the Hoya RPC service
    startHoyaRPCServer();

    String hostname = NetUtils.getConnectAddress(server).getHostName();
    appMasterHostname = hostname;
    appMasterRpcPort = server.getPort();
    appMasterTrackingUrl = null;
    log.info("HoyaAM Server is listening at {}:{}", appMasterHostname, appMasterRpcPort);

    //build the role map
    List<ProviderRole> providerRoles = provider.getRoles();
    for (ProviderRole providerRole : providerRoles) {
      //build role status map
      roleStatusMap.put(providerRole.key,
                        new RoleStatus(providerRole));
    }

    
    // work out a port for the AM
    int infoport = clusterSpec.getRoleOptInt(ROLE_MASTER,
                                                 RoleKeys.APP_INFOPORT,
                                                 0);
    if (0 == infoport) {
      infoport =
        HoyaUtils.findFreePort(HBaseConfigFileOptions.DEFAULT_MASTER_INFO_PORT,
                               128);
      //need to get this to the app
      
      clusterSpec.setRoleOpt(ROLE_MASTER,
                                     RoleKeys.APP_INFOPORT,
                                     infoport);
    }
    appMasterTrackingUrl =
      "http://" + appMasterHostname + ":" + infoport;


    // Register self with ResourceManager
    // This will start heartbeating to the RM
    address = HoyaUtils.getRmSchedulerAddress(asyncRMClient.getConfig());
    log.info("Connecting to RM at {},address tracking URL={}",
             appMasterRpcPort, appMasterTrackingUrl);
    RegisterApplicationMasterResponse response = asyncRMClient
      .registerApplicationMaster(appMasterHostname,
                                 appMasterRpcPort,
                                 appMasterTrackingUrl);
    configureContainerMemory(response);


    masterNode = new ClusterNode(hostname);
    masterNode.containerId = AppMasterContainerID;
    masterNode.role = ROLE_MASTER;
    masterNode.uuid = UUID.randomUUID().toString();


    //before bothering to start the containers, bring up the
    //hbase master.
    //This ensures that if the master doesn't come up, less
    //cluster resources get wasted

    //start hbase command

    File confDir = getLocalConfDir();
    if (!confDir.exists() || !confDir.isDirectory()) {

      throw new BadCommandArgumentsException(
        "Configuration directory " + confDir +
        " doesn't exist");
    }

    //now validate the dir by loading in a hadoop-site.xml file from it
    File hBaseSiteXML = new File(confDir, HBASE_SITE);
    if (!hBaseSiteXML.exists()) {
      StringBuilder builder = new StringBuilder();
      String[] confDirEntries = confDir.list();
      for (String entry : confDirEntries) {
        builder.append(entry).append("\n");
      }
      throw new FileNotFoundException(
        "Conf dir " + confDir + " doesn't contain " + HBASE_SITE +
        "\n" + builder);
    }

    //now read it in
    Configuration siteConf = ConfigHelper.loadConfFromFile(hBaseSiteXML);
    
    TreeSet<String> confKeys = ConfigHelper.sortedConfigKeys(siteConf);
    //update the values

    clusterSpec.zkHosts = siteConf.get(HBaseConfigFileOptions.KEY_ZOOKEEPER_QUORUM);
    clusterSpec.zkPort =
      siteConf.getInt(HBaseConfigFileOptions.KEY_ZOOKEEPER_PORT, 0);
    clusterSpec.zkPath = siteConf.get(HBaseConfigFileOptions.KEY_ZNODE_PARENT);

    noMaster = clusterSpec.getDesiredInstanceCount(
      ROLE_MASTER,1) <= 0;
    log.debug(" Contents of {}", hBaseSiteXML);

    for (String key : confKeys) {
      String val = siteConf.get(key);
      log.debug("{}={}", key, val);
      clusterSpec.clientProperties.put(key, val);
    }
    if (clusterSpec.zkPort == 0) {
      throw new BadCommandArgumentsException(
        "ZK port property not provided at %s in configuration file %s",
        HBaseConfigFileOptions.KEY_ZOOKEEPER_PORT,
        hBaseSiteXML);
    }

    //copy into cluster status. From here on spect should be frozen
    clusterStatus = ClusterDescription.copy(clusterSpec);


    clusterStatus.state = ClusterDescription.STATE_CREATED;
    clusterStatus.startTime = System.currentTimeMillis();
    if (0 == clusterStatus.createTime) {
      clusterStatus.createTime = clusterStatus.startTime;
    }
    clusterStatus.statusTime = System.currentTimeMillis();
    clusterStatus.state = ClusterDescription.STATE_LIVE;
    
    //look at settings of Hadoop Auth, to pick up a problem seen once
    checkAndWarnForAuthTokenProblems();


    if (noMaster) {
      log.info("skipping master launch");
      localProcessStarted = true;
    } else {
      addLaunchedContainer(AppMasterContainerID, masterNode);

      //pull out the command line argument if set
      String masterCommand =
        clusterSpec.getOption(HBaseConfigFileOptions.OPTION_HBASE_MASTER_COMMAND,
                                     MASTER);

      Map<String, String> env = new HashMap<String, String>();


      env.put(HoyaKeys.HBASE_LOG_DIR, new ProviderUtils(log).getLogdir());
      List<String> launchSequence = new ArrayList<String>(8);
      launchSequence.add(ARG_CONFIG);
      launchSequence.add(confDir.getAbsolutePath());
      launchSequence.add(masterCommand);
      launchSequence.add(ACTION_START);

      launchHBaseServer(clusterSpec,
                        launchSequence,
                        env);
    }

    try {
      //if we get here: success
      success = true;
      
      //here see if the launch worked.
      if (localProcessTerminated) {
        //exit without even starting a service
        log.info("Exiting early as process terminated with exit code {}",
                 spawnedProcessExitCode);
        return spawnedProcessExitCode;
      }

      //now ask for the cluster nodes
      flexClusterNodes(clusterSpec);

      //now block waiting to be told to exit the process
      waitForAMCompletionSignal();
      //shutdown time
    } finally {
      finish();
    }

    return success ? mapProcessExitCodeToYarnExitCode(spawnedProcessExitCode)
                   : EXIT_TASK_LAUNCH_FAILURE;
  }

  private void checkAndWarnForAuthTokenProblems() {
    String fileLocation = System.getenv(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
    if (fileLocation != null) {
      File tokenFile= new File(fileLocation);
      if (!tokenFile.exists()) {
        log.warn("Token file {} specified in {} not found",tokenFile,
                 UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
      }
    }
  }

  /**
   * Map an exit code from a process 
   * @param exitCode
   * @return an exit code
   */
  public int mapProcessExitCodeToYarnExitCode(int exitCode) {
    if (!spawnedProcessExitedBeforeShutdownTriggered) {
      //if triggered after shutdown, don't care about the exit code
      return 0;
    }
    //else
    switch (exitCode) {
      case 0:
        return 0;
      case 1:
        return EXIT_MASTER_PROCESS_FAILED;
      default:
        return exitCode;
    }
  }
  
  /**
   * Build the configuration directory passed in or of the target FS
   * @return the file
   */
  public File getLocalConfDir() {
    File confdir =
      new File(HoyaKeys.PROPAGATED_CONF_DIR_NAME).getAbsoluteFile();
    return confdir;
  }

  public String getDFSConfDir() {
    return clusterSpec.generatedConfigurationPath;
  }

  /**
   * Get the filesystem of this cluster
   * @return the FS of the config
   */
  public FileSystem getClusterFS() throws IOException {
    return FileSystem.get(getConfig());
  }


  /**
   * Block until it is signalled that the AM is done
   */
  private void waitForAMCompletionSignal() {
    AMExecutionStateLock.lock();
    try {
      if (!amCompletionFlag.get()) {
        log.debug("blocking until signalled to terminate");
        isAMCompleted.awaitUninterruptibly();
      }
    } finally {
      AMExecutionStateLock.unlock();
    }
    //add a sleep here for about a second. Why? it
    //stops RPC calls breaking so dramatically when the cluster
    //is torn down mid-RPC
    try {
      Thread.sleep(TERMINATION_SIGNAL_PROPAGATION_DELAY);
    } catch (InterruptedException ignored) {
      //ignored
    }
  }

  /**
   * Declare that the AM is complete
   */
  public void signalAMComplete(String reason) {
    log.info("Triggering shutdown of the AM: {}", reason);
    AMExecutionStateLock.lock();
    try {
      amCompletionFlag.set(true);
      isAMCompleted.signal();
    } finally {
      AMExecutionStateLock.unlock();
    }
  }

  /**
   * shut down the cluster 
   */
  private synchronized void finish() {
    //stop the daemon & grab its exit code
    Integer exitCode = stopForkedProcess();
    joinAllLaunchedThreads();


    log.info("Releasing all containers");
    //now release all containers
    releaseAllContainers();

    // When the application completes, it should send a finish application
    // signal to the RM
    log.info("Application completed. Signalling finish to RM");

    FinalApplicationStatus appStatus;
    String appMessage = null;
    success = true;
    String exitCodeString = exitCode != null ? exitCode.toString() : "n/a";
    if (numFailedContainers.get() == 0) {
      appStatus = FinalApplicationStatus.SUCCEEDED;
      appMessage = "completed. Local daemon exit code = " 
                   + exitCodeString;
    } else {
      appStatus = FinalApplicationStatus.FAILED;
      appMessage = "Completed with "+ numFailedContainers.get()+" failed containers: "
                   + " Local daemon exit code =  " +
                   exitCodeString + " - " + getContainerDiagnosticInfo();
      success = false;
    }
    try {
      log.info("Unregistering AM status={} message={}", appStatus, appMessage);
      asyncRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
    } catch (YarnException e) {
      log.info("Failed to unregister application: " + e, e);
    } catch (IOException e) {
      log.info("Failed to unregister application: " + e, e);
    }
    if (server != null) {
      server.stop();
    }
  }



  /**
   * Get diagnostics info about containers
   */
  private String getContainerDiagnosticInfo() {
    StringBuilder builder = new StringBuilder();
    for (RoleStatus roleStatus: roleStatusMap.values()) {
        builder.append(roleStatus).append('\n');
    }
    return builder.toString();
  }

  //TODO: rework
  private void configureContainerMemory(RegisterApplicationMasterResponse response) {
    synchronized (clusterSpecLock) {
      Resource maxResources =
        response.getMaximumResourceCapability();
      containerMemory = clusterSpec.getRoleOptInt(ROLE_WORKER,
                                                            RoleKeys.YARN_MEMORY,
                                                            DEFAULT_CONTAINER_MEMORY_FOR_WORKER);
      log.info("Setting container ask to {} from max of {}",
               containerMemory,
               maxResources);
    }
  }

  public Object getProxy(Class protocol, InetSocketAddress addr) {
    return rpc.getProxy(protocol, addr, getConfig());
  }

  /**
   * Register self as a server
   * @return the new server
   */
  private Server startHoyaRPCServer() throws IOException {
    server = new RPC.Builder(getConfig())
      .setProtocol(HoyaAppMasterProtocol.class)
      .setInstance(this)
      .setPort(0)
      .setNumHandlers(5)
//        .setSecretManager(sm)
      .build();
    server.start();

    return server;
  }

  /**
   * Setup the request that will be sent to the RM for the container ask.
   *
   *
   * @param role @return the setup ResourceRequest to be sent to RM
   */
  private AMRMClient.ContainerRequest buildContainerRequest(RoleStatus role) {
    // setup requirements for hosts
    // using * as any host initially
    String[] hosts = null;
    String[] racks = null;
    Priority pri = Records.newRecord(Priority.class);
    
    // Set up resource type requirements
    Resource capability = Records.newRecord(Resource.class);
    synchronized (clusterSpecLock) {
      // Set up resource requirements from role valuesx
      String name = role.getName();
      capability.setVirtualCores(clusterSpec.getRoleOptInt(name,
                                                                  YARN_CORES,
                                                                  DEF_YARN_CORES));
      capability.setMemory(clusterSpec.getRoleOptInt(name,
                                                            YARN_MEMORY,
                                                            DEF_YARN_MEMORY));
      pri.setPriority(role.getPriority());
    }
    AMRMClient.ContainerRequest request;
    request = new AMRMClient.ContainerRequest(capability,
                                              hosts,
                                              racks,
                                              pri,
                                              true);
    log.info("Requested container ask: {}", request);
    return request;
  }


  private void launchThread(RoleLauncher launcher, String name) {
    Thread launchThread = new Thread(launcherThreadGroup,
                                     launcher,
                                     name);

    // launch and start the container on a separate thread to keep
    // the main thread unblocked
    // as all containers may not be allocated at one go.
    synchronized (launchThreads) {
      launchThreads.put(launcher, launchThread);
    }
    launchThread.start();
  }

  /**
   * Method called by a launcher thread when it has completed; 
   * this removes the launcher of the map of active
   * launching threads.
   * @param launcher
   */
  public void launchedThreadCompleted(RoleLauncher launcher) {
    synchronized (launchThreads) {
      launchThreads.remove(launcher);
    }    
  }

  /**
   Join all launched threads
   needed for when we time out
   and we need to release containers
   */
  private void joinAllLaunchedThreads() {


    //first: take a snapshot of the thread list
    List<Thread> liveThreads;
    synchronized (launchThreads) {
      liveThreads = new ArrayList<Thread>(launchThreads.values());
    }
    log.info("Waiting for the completion of {} threads", liveThreads.size());
    for (Thread launchThread : liveThreads) {
      try {
        launchThread.join(LAUNCHER_THREAD_SHUTDOWN_TIME);
      } catch (InterruptedException e) {
        log.info("Exception thrown in thread join: " + e, e);
      }
    }
  }
  
  
  private int getRoleKey(Container c) {
    return c.getPriority().getPriority();
  }

  /**
   * Look up a role from its key -or fail 
   * 
   * @param key key to resolve
   * @return the status
   * @throws YarnRuntimeException on no match
   */
  private RoleStatus lookupRoleStatus(int key)  {
    RoleStatus rs = roleStatusMap.get(key);
    if (rs==null) {
      throw new YarnRuntimeException("Cannot find role for role key " + key);
    }
    return rs;
  }
  
  private RoleStatus lookupRoleStatus(String name) {
    for (RoleStatus roleStatus : roleStatusMap.values()) {
      if (roleStatus.getName().equals(name)) {
        return roleStatus;
      }
    }
    throw new YarnRuntimeException("Cannot find role for role " + name);

  }

  /**
   * Look up a role from its key -or fail 
   *
   * @param c container in a role
   * @return the status
   * @throws YarnRuntimeException on no match
   */
  private RoleStatus lookupRoleStatus(Container c) {
    return lookupRoleStatus(getRoleKey(c));
  }
  
/* =================================================================== */
/* AMRMClientAsync callbacks */
/* =================================================================== */

  /**
   * Callback event when a container is allocated
   * @param allocatedContainers list of containers that are now ready to be
   * given work
   */
  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  @Override //AMRMClientAsync
  public void onContainersAllocated(List<Container> allocatedContainers) {
    log.info("onContainersAllocated({})",
             allocatedContainers.size());
    List<Container> surplus = new ArrayList<Container>();
    for (Container container : allocatedContainers) {
      String containerHostInfo = container.getNodeId().getHost()
                                 + ":" +
                                 container.getNodeId().getPort();
      int allocated;
      int desired;
      //get the role
      RoleStatus role = lookupRoleStatus(container);
      synchronized (role) {
        //sync on all container details. Even though these are atomic,
        //we don't really want multiple updates happening simultaneously
        log.info(getContainerDiagnosticInfo());
        //dec requested count
        role.decRequested();
        //inc allocated count
        allocated =role.incActual();

        //look for (race condition) where we get more back than we asked
        desired = role.getDesired();
      }
      if ( allocated > desired) {
        log.info("Discarding surplus container {} on {}", container.getId(),
                 containerHostInfo);
        surplus.add(container);
      } else {
        
        log.info("Launching shell command on a new container.," +
                 " containerId={}," +
                 " containerNode={}:{}," +
                 " containerNodeURI={}," +
                 " containerResource={}",
                 container.getId(),
                 container.getNodeId().getHost(),
                 container.getNodeId().getPort(),
                 container.getNodeHttpAddress(),
                 container.getResource());

        String roleName = role.getName();
        RoleLauncher launcher =
          new RoleLauncher(this,
                           container,
                           roleName,
                           provider,
                           clusterSpec,
                           clusterSpec.getOrAddRole(
                             roleName));
        launchThread(launcher, "container-" +
                               containerHostInfo);
      }
    }
    //now discard those surplus containers
    for (Container container : surplus) {
      ContainerId id = container.getId();
      log.info("Releasing surplus container {} on {}:{}",
               id.getApplicationAttemptId(),
               container.getNodeId().getHost(),
               container.getNodeId().getPort());
      RoleStatus role = lookupRoleStatus(container);
      synchronized (role) {
        role.incReleasing();
      }
      asyncRMClient.releaseAssignedContainer(id);
    }
    log.info("Diagnostics: " + getContainerDiagnosticInfo());
  }

  @Override //AMRMClientAsync
  public synchronized void onContainersCompleted(List<ContainerStatus> completedContainers) {
    log.info("onContainersCompleted([{}]", completedContainers.size());
    for (ContainerStatus status : completedContainers) {
      ContainerId containerId = status.getContainerId();
      log.info("Container Completion for" +
               " containerID={}," +
               " state={}," +
               " exitStatus={}," +
               " diagnostics={}",
               containerId, status.getState(),
               status.getExitStatus(),
               status.getDiagnostics());

      // non complete containers should not be here
      assert (status.getState() == ContainerState.COMPLETE);

      if (containersBeingReleased.containsKey(containerId)) {
        log.info("Container was queued for release");
        Container container = containersBeingReleased.remove(containerId);
        RoleStatus roleStatus = lookupRoleStatus(container);
        synchronized (roleStatus) {
          roleStatus.decReleasing();
          roleStatus.decActual();
        }
      } else {
        //a container has failed and its role needs to be decremented
        ContainerInfo containerInfo = containers.remove(containerId);
        if (containerInfo != null) {
          Container container = containerInfo.container;
          String rolename = containerInfo.role;
          log.info("Failed container in role {}", rolename);
          RoleStatus roleStatus = lookupRoleStatus(rolename);
          if (roleStatus != null) {
            roleStatus.decActual();
          } else {
            log.error("Failed container of unknown role {}", rolename);
          }
        } else {
          log.error("Notified of completed container that is not in the list" +
                    "of active containers");
        }
        //record the complete node's details; this pulls it from the livenode set 
        updateCompletedNode(status);
      }
    }

    // ask for more containers if any failed
    // In the case of Hoya, we don't expect containers to complete since
    // Hoya is a long running application. Keep track of how many containers
    // are completing. If too many complete, abort the application
    // TODO: this needs to be better thought about (and maybe something to
    // better handle in Yarn for long running apps)

    reviewRequestAndReleaseNodes();

  }

  /**
   * How many failures to tolerate
   * On test runs, the numbers are low to keep things under control
   * @return the max #of failures
   */
  public int maximumContainerFailureLimit() {

    return clusterSpec.getOptionBool(OptionKeys.OPTION_TEST,false) ? 1 : MAX_TOLERABLE_FAILURES;
  }


  /**
   * Implementation of cluster flexing.
   * This is synchronized so that it doesn't get confused by other requests coming
   * in.
   * It should be the only way that anything -even the AM itself on startup-
   * asks for nodes. 
   * @param workers #of workers to add
   * @param masters #of masters to request (if supported)
   * @return true if the number of workers changed
   * @throws IOException
   */
  private boolean flexClusterNodes(ClusterDescription updated) throws
                                                             IOException {

    synchronized (clusterSpecLock) {
      //we assume that the spec is valid
      //TODO: more validation?
      clusterSpec = updated;

      //propagate info from cluster, which is role table

      Map<String, Map<String, String>> roles = clusterSpec.roles;
      Map<String, Map<String, String>> roleclone = clusterSpec.roles;
      for (Map.Entry<String, Map<String, String>> entry : roles.entrySet()) {
        Map<String, String> map = new HashMap<String, String>(entry.getValue());
        roleclone.put(entry.getKey(), map);
      }
      clusterStatus.roles = roleclone;
      clusterStatus.updateTime = System.currentTimeMillis();
      
      //now update every role's desired count.
      //if there are no instance values, that role count goes to zero
      for (RoleStatus roleStatus : roleStatusMap.values()) {
        synchronized (roleStatus) {
          roleStatus.setDesired(clusterSpec.getDesiredInstanceCount(roleStatus.getName(), 0));
        }
      }
    }

    // ask for more containers if needed
    return reviewRequestAndReleaseNodes();
  }

  /**
   * Look at where the current node state is -and whether it should be changed
   */
  private synchronized boolean reviewRequestAndReleaseNodes() {
    log.debug("in reviewRequestAndReleaseNodes()");
    if (amCompletionFlag.get()) {
      log.info("Ignoring node review operation: shutdown in progress");
      return false;
    }
    
    boolean updatedNodeCount = false;

    for (RoleStatus roleStatus : roleStatusMap.values()) {
      if (!roleStatus.getExcludeFromFlexing()) {
        updatedNodeCount |= reviewOneRole(roleStatus);
      }
    }
    return updatedNodeCount;
  }

  private boolean reviewOneRole(RoleStatus role) {
    int delta;
    String details;
    int expected;
    synchronized (role) {
      delta = role.getDelta();
      details = role.toString();
      expected = role.getDesired();
    }

    log.info(details);
    boolean updated = false;
    if (delta > 0) {
      log.info("Asking for {} more worker(s) for a total of {} ",
               delta, expected);
      //more workers needed than we have -ask for more
      for (int i = 0; i < delta; i++) {
        AMRMClient.ContainerRequest containerAsk =
          buildContainerRequest(role);
        log.info("Container ask is {}", containerAsk);
        synchronized (role) {
          role.incRequested();
        }
        asyncRMClient.addContainerRequest(containerAsk);
      }
      updated = true;
    } else if (delta < 0) {

      //special case: there are no more containers
/*
      if (total == 0 && !noMaster) {
        //just exit the entire application here, rather than a node at a time.
        signalAMComplete("#of workers is set to zero: exiting");
        return;
      }
*/

      log.info("Asking for {} fewer worker(s) for a total of {}", -delta, expected);
      //reduce the number expected (i.e. subtract the delta)
//      numRequestedContainers.addAndGet(delta);

      //then pick some containers to kill
      int excess = -delta;
      Collection<ContainerInfo> targets = containers.values();
      for (ContainerInfo ci : targets) {
        if (excess > 0) {
          Container possible = ci.container;
          ContainerId id = possible.getId();
          if (!ci.released) {
            log.info("Requesting release of container {}", id);
            ci.released = true;
            containersBeingReleased.put(id, possible);
            synchronized (role) {
              role.incReleasing();
            }
            asyncRMClient.releaseAssignedContainer(id);
            excess--;
          }
        }
      }
      //here everything should be freed up, though there may be an excess due 
      //to race conditions with requests coming in
      if (excess > 0) {
        log.warn(
          "After releasing all worker nodes that could be free, there was an excess of {} nodes",
          excess);
      }
      updated = true;

    }
    return updated;
  }

  /**
   * Shutdown operation: release all containers
   */
  void releaseAllContainers() {
    Collection<ContainerInfo> targets = containers.values();
    for (ContainerInfo ci : targets) {
        Container possible = ci.container;
        ContainerId id = possible.getId();
        if (!ci.released) {
          log.info("Requesting release of container {}", id);
          ci.released = true;
          containersBeingReleased.put(id, possible);
          RoleStatus roleStatus = lookupRoleStatus(possible);
          synchronized (roleStatus) {
            roleStatus.incReleasing();
            roleStatus.getDesired();
          }
          asyncRMClient.releaseAssignedContainer(id);
        }
      }
  }
  
  /**
   * RM wants to shut down the AM
   */
  @Override //AMRMClientAsync
  public void onShutdownRequest() {
    signalAMComplete("Shutdown requested from RM");
  }

  /**
   * Monitored nodes have been changed
   * @param updatedNodes list of updated notes
   */
  @Override //AMRMClientAsync
  public void onNodesUpdated(List<NodeReport> updatedNodes) {
    log.info("Nodes updated");
  }

  /**
   * heartbeat operation; return the ratio of requested
   * to actual
   * @return progress
   */
  @Override //AMRMClientAsync
  public float getProgress() {
    float percentage = 0;
    int desired =0 ;
    float actual = 0;
    for (RoleStatus role : roleStatusMap.values()) {
      synchronized (role) {
        desired += role.getDesired();
        actual += role.getActual();
      }
    }
    if (desired == 0) {
      percentage = 100;
    } else {
      percentage = actual / desired;
    }
    log.debug("Heartbeat, percentage ={}", percentage);
    return percentage;
  }

  @Override //AMRMClientAsync
  public void onError(Throwable e) {
    //callback says it's time to finish
    log.error("AMRMClientAsync.onError() received " + e, e);
    signalAMComplete("AMRMClientAsync.onError() received " + e);
  }
  
/* =================================================================== */
/* HoyaAppMasterApi */
/* =================================================================== */

  @Override   //HoyaAppMasterApi
  public ProtocolSignature getProtocolSignature(String protocol,
                                                long clientVersion,
                                                int clientMethodsHash) throws
                                                                       IOException {
    return ProtocolSignature.getProtocolSignature(
      this, protocol, clientVersion, clientMethodsHash);
  }

  @Override   //HoyaAppMasterApi
  public void stopCluster() throws IOException {
    log.info("HoyaAppMasterApi.stopCluster()");
    signalAMComplete("stopCluster() invoked");
  }

  @Override   //HoyaAppMasterApi
  public boolean flexCluster(String clusterSpec) throws IOException {
    ClusterDescription updated =
      ClusterDescription.fromJson(clusterSpec);
    return flexClusterNodes(updated);
  }


  @Override   //HoyaAppMasterApi
  public long getProtocolVersion(String protocol, long clientVersion) throws
                                                                      IOException {
    return versionID;
  }

  @Override //HoyaAppMasterApi
  public synchronized String getClusterStatus() throws IOException {
    updateClusterStatus();
    return clusterStatus.toJsonString();
  }

  @Override
  public String[] listNodesByRole(String role) {
    List<ClusterNode> nodes = enumNodesByRole(role);
    String[] result = new String[nodes.size()];
    int count = 0;
    for (ClusterNode node: nodes) {
      result[count++] = node.uuid;
    }
    return result;
  }

  public List<ClusterNode> enumNodesByRole(String role) {
    List<ClusterNode> nodes = new ArrayList<ClusterNode>();
    synchronized (clusterSpecLock) {
      for (ClusterNode node : liveNodes.values()) {
        if (role.equals(node.role)) {
          nodes.add(node);
        }
      }
    }
    return nodes;
  }

  @Override
  public String getNode(String uuid) throws IOException, NoSuchNodeException {
    //todo: optimise
    synchronized (clusterSpecLock) {
      for (ClusterNode node : liveNodes.values()) {
        if (uuid.equals(node.uuid)) {
          return node.toJsonString();
        }
      }
    }
    //at this point: no node
    throw new NoSuchNodeException(uuid);
  }

  
/* =================================================================== */
/* END */
/* =================================================================== */

  /**
   * Update the cluster description with anything interesting
   */
  private void updateClusterStatus() {


    long t = System.currentTimeMillis();
    synchronized (clusterSpecLock) {
      clusterStatus.statusTime = t;
      if (masterNode != null) {
        if (hbaseMaster != null) {
          masterNode.command = HoyaUtils.join(hbaseMaster.getCommands(), " ");
          if (hbaseMaster.isRunning()) {
            masterNode.state = ClusterDescription.STATE_LIVE;
          } else {
            masterNode.state = ClusterDescription.STATE_STOPPED;
            masterNode.diagnostics = "Exit code = " + hbaseMaster.getExitCode();
          }
          //pull in recent lines of output from the HBase master
          List<String> output = hbaseMaster.getRecentOutput();
          masterNode.output = output.toArray(new String[output.size()]);
        } else {
          masterNode.state = ClusterDescription.STATE_DESTROYED;
          masterNode.output = new String[0];
        }
      }
      clusterStatus.stats = new HashMap<String, Map<String, Integer>>();
      for (RoleStatus role : roleStatusMap.values()) {
        String rolename = role.getName();
        List<ClusterNode> nodes = enumNodesByRole(rolename);
        int nodeCount = nodes.size();
        clusterStatus.setActualInstanceCount(rolename, nodeCount);
        clusterStatus.instances = buildInstanceMap();
        Map<String, Integer> stats = new HashMap<String, Integer>();
        stats.put(STAT_CONTAINERS_REQUESTED, role.getRequested());
        stats.put(STAT_CONTAINERS_ALLOCATED, role.getActual());
        stats.put(STAT_CONTAINERS_COMPLETED, role.getCompleted());
        stats.put(STAT_CONTAINERS_FAILED, role.getFailed());
        stats.put(STAT_CONTAINERS_STARTED, role.getStarted());
        stats.put(STAT_CONTAINERS_STARTED_FAILED, role.getStartFailed());
        clusterStatus.stats.put(rolename, stats);
      }
    }
  }

  /**
   * Build an instance map.
   * This code does not acquire any locks and is not thread safe; caller is
   * expected to hold the lock.
   * @return the map of instance -> count
   */
  private Map<String, Integer> buildInstanceMap() {
    Map<String, Integer> map = new HashMap<String, Integer>();
    for (ClusterNode node : liveNodes.values()) {
      Integer entry = map.get(node.role);
      int current = entry != null ? entry : 0;
      current++;
      map.put(node.role, current);
    }
    return map;
  }
  
  /**
   * handle completed node in the CD -move something from the live
   * server list to the completed server list
   * @param completed the node that has just completed
   */
  private void updateCompletedNode(ContainerStatus completed) {

    //add the node
    synchronized (clusterSpecLock) {
      ContainerId id = completed.getContainerId();
      ClusterNode node = liveNodes.remove(id);
      if (node == null) {
        node = new ClusterNode();
        node.name = id.toString();
        node.containerId = id;
      }
      node.state = ClusterDescription.STATE_DESTROYED;
      node.exitCode = completed.getExitStatus();
      node.diagnostics = completed.getDiagnostics();
      completedNodes.put(id, node);
    }
  }

  /**
   * add a launched container to the node map for status responss
   * @param id id
   * @param node node details
   */
  public void addLaunchedContainer(ContainerId id, ClusterNode node) {
    node.containerId = id;
    if (node.role == null) {
      log.warn("Unknown role for node {}", node);
      node.role = ROLE_UNKNOWN;
    }
    if (node.uuid==null) {
      node.uuid = UUID.randomUUID().toString();
      log.warn("creating UUID for node {}", node);
    }
    synchronized (clusterSpecLock) {
      liveNodes.put(node.containerId, node);
    }

  }

  /**
   * Launch the hbase server
   * @param commands list of commands -bin/hbase is inserted on the front
   * @param env environment variables above those generated by
   * @throws IOException IO problems
   * @throws HoyaException anything internal
   */
  protected synchronized void launchHBaseServer(ClusterDescription cd,
                                                List<String> commands,
                                                Map<String, String> env)
    throws IOException, HoyaException {
    if (hbaseMaster != null) {
      throw new HoyaInternalStateException("trying to launch hbase server" +
                                           " when one is already running");
    }
    //prepend the hbase command itself
    File binHbaseSh = buildHBaseBinPath(cd);
    String scriptPath = binHbaseSh.getAbsolutePath();
    if (!binHbaseSh.exists()) {
      throw new BadCommandArgumentsException("Missing script " + scriptPath);
    }
    commands.add(0, scriptPath);
    hbaseMaster = new RunLongLivedApp(LOG_AM_PROCESS, commands);
    hbaseMaster.setApplicationEventHandler(this);
    //set the env variable mapping
    hbaseMaster.putEnvMap(env);

    //now spawn the process -expect updates via callbacks
    hbaseMaster.spawnApplication();

  }

  @Override // ApplicationEventHandler
  public void onApplicationStarted(RunLongLivedApp application) {
    log.info("Process has started");
    localProcessStarted = true;
    synchronized (clusterSpecLock) {
      masterNode.state = ClusterDescription.STATE_LIVE;
    }
  }

  /**
   * This is the callback on the HBaseMaster process 
   * -it's raised when that process terminates
   * @param application application
   * @param exitCode exit code
   */
  @Override // ApplicationEventHandler
  public void onApplicationExited(RunLongLivedApp application, int exitCode) {
    localProcessTerminated = true;
    synchronized (clusterSpecLock) {
      spawnedProcessExitCode = exitCode;
      //did the process exit before the AM completion flag? If so
      //a failure may be relevant
      spawnedProcessExitedBeforeShutdownTriggered = ! amCompletionFlag.get();
      masterNode.exitCode = spawnedProcessExitCode;
      masterNode.state = ClusterDescription.STATE_STOPPED;
    }
    log.info("Process has exited with exit code {}, beforeShutdownTriggered={}",
             exitCode,
             spawnedProcessExitedBeforeShutdownTriggered);

    //tell the AM the cluster is complete 
    signalAMComplete("Spawned master exited with " + exitCode);
  }
  

  
  /**
   * Get the path to hbase home
   * @return the hbase home path
   */
  public File buildHBaseBinPath(ClusterDescription cd) {
    return HBaseProvider.buildHBaseBinPath(cd);
  }

  /**
   * stop hbase process if it the running process var is not null
   * @return the hbase exit code -null if it is not running
   */
  protected synchronized Integer stopForkedProcess() {
    Integer exitCode;
    if (hbaseMaster != null) {
      hbaseMaster.stop();
      exitCode = hbaseMaster.getExitCode();
      hbaseMaster = null;
    } else {
      exitCode = null;
    }
    return exitCode;
  }

  /**
   * Add a property to the hbase client properties list in the
   * cluster description
   * @param key property key
   * @param val property value
   */
  public void noteHBaseClientProperty(String key, String val) {
    synchronized (clusterSpecLock) {
      clusterStatus.clientProperties.put(key, val);
    }
  }

  public void startContainer(Container container,
                             ContainerLaunchContext ctx,
                             ClusterNode node) {
    node.state = ClusterDescription.STATE_SUBMITTED;
    node.containerId = container.getId();
    synchronized (clusterSpecLock) {
      requestedNodes.put(container.getId(), node);
    }
    ContainerInfo containerInfo = new ContainerInfo();
    containerInfo.container = container;
    containerInfo.role = node.role;
    containerInfo.createTime = System.currentTimeMillis();
    containers.putIfAbsent(container.getId(), containerInfo);
    nmClientAsync.startContainerAsync(container, ctx);
  }

  @Override //  NMClientAsync.CallbackHandler 
  public void onContainerStopped(ContainerId containerId) {
    log.info("onContainerStopped {} ", containerId);
    //Removing live container?
/*    synchronized (clusterSpecLock) {
      containers.remove(containerId);
      ClusterNode node = liveNodes.remove(containerId);
      if (node != null) {
        completedNodes.put(containerId, node);
      }
    }*/
  }

  @Override //  NMClientAsync.CallbackHandler 
  public void onContainerStarted(ContainerId containerId,
                                 Map<String, ByteBuffer> allServiceResponse) {
    log.debug("Started Container {} ", containerId);
    startedContainers.incrementAndGet();
    ContainerInfo cinfo = null;
    //update the model
    synchronized (clusterSpecLock) {
      ClusterNode node = requestedNodes.remove(containerId);
      if (null == node) {
        log.warn("Creating a new node description for an unrequested node");
        node = new ClusterNode(containerId.toString());
        node.role = ROLE_UNKNOWN;
      }
      node.state = ClusterDescription.STATE_LIVE;
      node.uuid = UUID.randomUUID().toString();
      addLaunchedContainer(containerId, node);
      cinfo = containers.get(containerId);
    }
    if (cinfo != null) {
      cinfo.startTime = System.currentTimeMillis();
      //trigger an async container status
      nmClientAsync.getContainerStatusAsync(containerId,
                                            cinfo.container.getNodeId());
    } else {
      //this is a hypothetical path not seen. We react by warning
      //there's not much else to do
      log.error("Notified of started container that isn't pending {}",
                containerId);
    }
  }

  @Override //  NMClientAsync.CallbackHandler 
  public void onStartContainerError(ContainerId containerId, Throwable t) {
    log.error("Failed to start Container " + containerId, t);
    containers.remove(containerId);
    numFailedContainers.incrementAndGet();
    startFailedContainers.incrementAndGet();
    synchronized (clusterSpecLock) {
      ClusterNode node = requestedNodes.remove(containerId);
      if (null != node) {
        if (null != t) {
          node.diagnostics = HoyaUtils.stringify(t);
        }
        failedNodes.put(containerId, node);
      }
    }
  }

  @Override //  NMClientAsync.CallbackHandler 
  public void onContainerStatusReceived(ContainerId containerId,
                                        ContainerStatus containerStatus) {
    log.debug("Container Status: id={}, status={}", containerId,
              containerStatus);
  }

  @Override //  NMClientAsync.CallbackHandler 
  public void onGetContainerStatusError(
    ContainerId containerId, Throwable t) {
    log.error("Failed to query the status of Container {}", containerId);
  }


  //TODO: what handling should we be doing here vs. RM notifications?
  @Override //  NMClientAsync.CallbackHandler 
  public void onStopContainerError(ContainerId containerId, Throwable t) {
    log.error("Failed to stop Container {}", containerId);
/*
    containers.remove(containerId);
    ClusterNode node = failNode(containerId, t);
*/
  }

  /**
   * Move a node from the live set to the failed list
   * @param containerId container ID to look for
   * @param nodeList list to scan from (& remove found)
   * @return the node, if found
   */
  public ClusterNode failNode(ContainerId containerId,
                              Throwable t) {
    ClusterNode node;
    synchronized (clusterSpecLock) {
     node =  liveNodes.remove(containerId);

      if (node != null) {
        if (t != null) {
          node.diagnostics = HoyaUtils.stringify(t);
        }
        failedNodes.put(containerId, node);
      }
    }
    return node;
  }
}
