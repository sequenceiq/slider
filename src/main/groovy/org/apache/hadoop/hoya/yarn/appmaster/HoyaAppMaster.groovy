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

package org.apache.hadoop.hoya.yarn.appmaster

import groovy.transform.CompileStatic
import groovy.util.logging.Commons
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hoya.HBaseCommands
import org.apache.hadoop.hoya.api.ClusterDescription
import org.apache.hadoop.hoya.exceptions.HoyaInternalStateException
import org.apache.hadoop.hoya.tools.HoyaUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.hoya.HoyaApp
import org.apache.hadoop.hoya.HoyaExitCodes
import org.apache.hadoop.hoya.api.HoyaAppMasterProtocol
import org.apache.hadoop.hoya.exceptions.HoyaException
import org.apache.hadoop.hoya.exec.RunLongLivedApp
import org.apache.hadoop.hoya.tools.Env
import org.apache.hadoop.hoya.tools.YarnUtils
import org.apache.hadoop.hoya.yarn.client.ClientArgs
import org.apache.hadoop.ipc.ProtocolSignature
import org.apache.hadoop.ipc.RPC
import org.apache.hadoop.ipc.Server
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ContainerExitStatus
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId
import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.api.records.ContainerId
import org.apache.hadoop.yarn.api.records.ContainerState
import org.apache.hadoop.yarn.api.records.ContainerStatus
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import org.apache.hadoop.yarn.api.records.NodeReport
import org.apache.hadoop.yarn.api.records.Priority
import org.apache.hadoop.yarn.api.records.Resource
import org.apache.hadoop.yarn.client.AMRMClient
import org.apache.hadoop.yarn.client.AMRMClientAsync
import org.apache.hadoop.yarn.client.AMRMClientImpl
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.exceptions.YarnException
import org.apache.hadoop.yarn.ipc.YarnRPC
import org.apache.hadoop.yarn.service.CompositeService
import org.apache.hadoop.yarn.service.launcher.RunService
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.hadoop.yarn.util.Records

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock

/**
 * The AM for Hoya
 */
@Commons
//@CompileStatic

class HoyaAppMaster extends CompositeService
    implements AMRMClientAsync.CallbackHandler,
      RunService,
      HoyaExitCodes,
      HoyaAppMasterProtocol {

  // YARN RPC to communicate with the Resource Manager or Node Manager
  public static final int LAUNCHER_THREAD_SHUTDOWN_TIME = 10000
  private YarnRPC rpc;
  // Handle to communicate with the Resource Manager
  private AMRMClientAsync asyncRMClient;
  
  //RPC server
  private Server server; 
  // Hostname of the container
  private String appMasterHostname = "";
  // Port on which the app master listens for status updates from clients
  private int appMasterRpcPort = 0;
  // Tracking url to which app master publishes info for clients to monitor
  private String appMasterTrackingUrl = "";

  // Application Attempt Id ( combination of attemptId and fail count )
  private ApplicationAttemptId appAttemptID;
  // App Master configuration
  // No. of containers to run shell command on
  private int numTotalContainers = 1; //HACK to test
  // Memory to request for the container on which the shell command will run
  private int containerMemory = 10;
  // Priority of the request
  private int requestPriority;
  // Counter for completed containers ( complete denotes successful or failed )
  private final AtomicInteger numCompletedContainers = new AtomicInteger();
  // Allocated container count so that we know how many containers has the RM
  // allocated to us
  private final AtomicInteger numAllocatedContainers = new AtomicInteger();
  // Count of failed containers
  private final AtomicInteger numFailedContainers = new AtomicInteger();
  // Count of containers already requested from the RM
  // Needed as once requested, we should not request for containers again.
  // Only request for more if the original requirement changes.
  private final AtomicInteger numRequestedContainers = new AtomicInteger();

  /**
   * Command to launch
   */
  private String hbaseCommand = HBaseCommands.MASTER

  // Launch threads
  private final List<Thread> launchThreads = new ArrayList<Thread>();
  final ThreadGroup launcherThreadGroup = new ThreadGroup("launcher");

  /**
   * model the state using locks and conditions
   */
  private final ReentrantLock AMExecutionStateLock = new ReentrantLock()
  final Condition isAMCompleted = AMExecutionStateLock.newCondition();
  private volatile boolean success; 

  String[] argv
  private HoyaMasterServiceArgs serviceArgs
  private float progressCounter = 0.0f
  private final ClusterDescription clusterDescription = new ClusterDescription();
  //hbase command
  private RunLongLivedApp hbaseMaster
  private ClusterDescription.ClusterNode masterNode;


  public HoyaAppMaster() {
    super("HoyaMasterService")
    new HoyaApp("HoyaMasterService")
  }

  @Override
  public void setArgs(String...args) {
    this.argv = args;
    serviceArgs = new HoyaMasterServiceArgs(argv)
    serviceArgs.parse()
    serviceArgs.postProcess()
  }

  @Override
  synchronized void init(Configuration conf) {
    //sort out the location of the AM
    serviceArgs.applyDefinitions(conf);
    String rmAddress = serviceArgs.rmAddress
    YarnUtils.setRmSchedulerAddress(conf, rmAddress)
//    YarnUtils.setRmAddressGlobal(rmAddress);
    super.init(conf)
  }

  
/**
 * this is where the work is done.
 * @return the exit code
 * @throws Throwable
 */
  @Override
  public int runService() throws Throwable {

    //choose the action
    String action = serviceArgs.action
    List<String> actionArgs = serviceArgs.actionArgs
    int exitCode = EXIT_SUCCESS
    switch (action) {

      case ClientArgs.ACTION_HELP:
        log.info(getName() + serviceArgs.usage())
        break;

      case ClientArgs.ACTION_CREATE:
        exitCode = createAndRunCluster(actionArgs[0])
        break;

      default:
        throw new HoyaException("Unimplemented: " + action)
    }
    return exitCode
  }
  
  /**
   * Create and run the cluster
   * @return exit code
   * @throws Throwable on a failure
   */
  public int createAndRunCluster(String clustername) throws Throwable {
    clusterDescription.name = clustername;
    clusterDescription.state = ClusterDescription.STATE_CREATED;
    clusterDescription.startTime = System.currentTimeMillis();
    clusterDescription.zkConnection = serviceArgs.zookeeper
    clusterDescription.zkPath = serviceArgs.hbasezkpath
    
    YarnConfiguration conf = new YarnConfiguration(config);

    InetSocketAddress address = YarnUtils.getRmSchedulerAddress(conf)
    log.info("RM is at $address")
    rpc = YarnRPC.create(conf);
//    AMRMProtocol proxy = (AMRMProtocol)rpc.getProxy(AMRMProtocol, address, conf);

    ContainerId containerId = ConverterUtils.toContainerId(
        Env.mandatory(ApplicationConstants.Environment.CONTAINER_ID.name()));
    appAttemptID = containerId.applicationAttemptId;

    String nmHost = Env.mandatory(ApplicationConstants.Environment.NM_HOST.name())
    String nmPort = Env.mandatory(ApplicationConstants.Environment.NM_PORT.name())
    String nmHttpPort = Env.mandatory(ApplicationConstants.Environment.NM_HTTP_PORT.name())
    String UserName = Env.mandatory(ApplicationConstants.Environment.USER
                                                           .key());

    log.info("Hoya AM for app," +
             " appId=$appAttemptID.applicationId.id," +
             " clustertimestamp=$appAttemptID.applicationId.clusterTimestamp," +
             " attemptId=$appAttemptID.attemptId");


    int heartbeatInterval = 1000
    AMRMClient<AMRMClient.ContainerRequest> rmClient =
      new AMRMClientImpl<AMRMClient.ContainerRequest>(appAttemptID) 
    //add the RM client -this brings the callbacks in
    asyncRMClient = new AMRMClientAsync<AMRMClient.ContainerRequest>(rmClient,
                                                                     heartbeatInterval,
                                                                     this);

    //add to the list of things to terminate
    addService(asyncRMClient)
    //now bring it up
    asyncRMClient.init(conf);
    asyncRMClient.start();
    

    startAMActionsServer();
    
    String hostname = NetUtils.getConnectAddress(server).hostName
    appMasterHostname = hostname ;
    appMasterRpcPort = server.port;
    appMasterTrackingUrl = null;
    log.info("Server is at $appMasterHostname:$appMasterRpcPort")

    // Setup local RPC Server to accept status requests directly from clients
    // TODO need to setup a protocol for client to be able to communicate to
    // the RPC server
    // TODO use the rpc port info to register with the RM for the client to
    // send requests to this app master

    // Register self with ResourceManager
    // This will start heartbeating to the RM
    address = YarnUtils.getRmSchedulerAddress(rmClient.config)
    log.info("Connecting to RM at $address")
    RegisterApplicationMasterResponse response = asyncRMClient
        .registerApplicationMaster(appMasterHostname,
                                   appMasterRpcPort,
                                   appMasterTrackingUrl);
    configureContainerMemory(response)
    log.info("Total containers in this app " + numTotalContainers)

    // Setup ask for containers from RM
    // Send request for containers to RM
    // Until we get our fully allocated quota, we keep on polling RM for
    // containers
    // Keep looping until all the containers are launched and shell script
    // executed on them ( regardless of success/failure).
    AMRMClient.ContainerRequest containerAsk = setupContainerAskForRM(numTotalContainers);
    asyncRMClient.addContainerRequest(containerAsk);
    numRequestedContainers.set(numTotalContainers);

    //start hbase command
    //pull out the command line argument if set
    if (serviceArgs.hbaseCommand != null) {
      hbaseCommand = serviceArgs.hbaseCommand;
    }

    List<String> launchSequence = [HBaseCommands.ARG_CONFIG, buildConfDir()];
    launchSequence << hbaseCommand
    launchSequence << HBaseCommands.ACTION_START;
    
    //launchSequence <<  serviceArgs.hbaseCommand
    if (serviceArgs.xNoMaster) {
      log.info "skipping master launch as xNoMaster is set"
    } else {
      launchHBaseServer(launchSequence,
                        ["HBASE_LOG_DIR": buildHBaseLogdir()]);
    }
    
    //if we get here: success
    success = true;
    clusterDescription.state= ClusterDescription.STATE_STARTED;
    clusterDescription.maxMasterNodes = clusterDescription.minMasterNodes = 1;
    masterNode = new ClusterDescription.ClusterNode(hostname)
    clusterDescription.masterNodes = [
        masterNode
    ]

    waitForAMCompletionSignal()
    finish();

    return success ? EXIT_SUCCESS : EXIT_TASK_LAUNCH_FAILURE;
  }

  public String buildConfDir() {
    String confdir = new File(serviceArgs.hbasehome, "/conf").absolutePath;
    if (serviceArgs.confdir) {
      confdir = serviceArgs.confdir
      //"/Users/ddas/workspace/confYarnHBase";
    }
    return HoyaUtils.quoteArg(confdir)
  }

  /**
   * build the log directory
   * @return
   */
  public String buildHBaseLogdir() {
    String logdir = System.getenv("LOGDIR");
    if (!logdir) {
      logdir = "/tmp/hoya-" + UserGroupInformation.getCurrentUser().getShortUserName();
    }
    return logdir
  }

  /**
   * Build the log dir env variable for the containers
   * @return
   */
  public String buildHBaseContainerLogdir() {
    return buildHBaseLogdir();
  }

  /**
   * Block until it is signalled that the AM is done
   */
  private void waitForAMCompletionSignal() {
    AMExecutionStateLock.lock()
    try {
      isAMCompleted.awaitUninterruptibly();
    } finally {
      AMExecutionStateLock.unlock()
    }
    //add a sleep here for about a second. Why? it
    //stops RPC calls breaking so dramatically
    try {
      Thread.sleep(1000)
    } catch (InterruptedException ignored) {
    }
  }

  /**
   * Declare that the AM is complete
   */
  public void signalAMComplete() {
    AMExecutionStateLock.lock()
    try {
      isAMCompleted.signal()
    } finally {
      AMExecutionStateLock.unlock()
    }
  }

  /**
   * shut down the cluster 
   */
  private void finish() {
    //stop the daemon & grab its exit code
    Integer exitCode
    if (hbaseMaster) {
      hbaseMaster.stop();
      exitCode = hbaseMaster.exitCode
    } else {
      exitCode = null;
    }
    
    // Join all launched threads
    // needed for when we time out
    // and we need to release containers
    
    //first: take a snapshot of the thread list
    List<Thread> liveThreads
    synchronized (launchThreads) {
      liveThreads = new ArrayList<Thread>(launchThreads)
    }
    log.info("Waiting for the completion of ${liveThreads.size()} threads")
    for (Thread launchThread : liveThreads) {
      try {
        launchThread.join(LAUNCHER_THREAD_SHUTDOWN_TIME);
      } catch (InterruptedException e) {
        log.info("Exception thrown in thread join: $e", e);
      }
    }

    // When the application completes, it should send a finish application
    // signal to the RM
    log.info("Application completed. Signalling finish to RM");

    FinalApplicationStatus appStatus;
    String appMessage = null;
    success = true;
    if (numFailedContainers.get() == 0) {
      appStatus = FinalApplicationStatus.SUCCEEDED;
      appMessage = "completed. Master exit code = $exitCode"
    } else {
      appStatus = FinalApplicationStatus.FAILED;
      appMessage = "Diagnostics" +
                    "Master exit code = $exitCode," +
                   " total=$numTotalContainers," +
                   " completed=${numCompletedContainers.get()}," +
                   " allocated=${numAllocatedContainers.get()}," +
                   " failed=${numFailedContainers.get()}";
      success = false;
    }
    try {
      log.info("Unregistering AM status=$appStatus message=$appMessage")
      asyncRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
    } catch (YarnException e) {
      log.error("Failed to unregister application: $e", e);
    } catch (IOException e) {
      log.error("Failed to unregister application: $e", e);
    }
    server?.stop()
  }

  private void configureContainerMemory(RegisterApplicationMasterResponse response) {
    int minMem = response.minimumResourceCapability.memory;
    int maxMem = response.maximumResourceCapability.memory;
    log.info("Min mem capability of resources in this cluster $minMem");
    log.info("Max mem capability of resources in this cluster $maxMem");

    // A resource ask has to be atleast the minimum of the capability of the
    // cluster, the value has to be a multiple of the min value and cannot
    // exceed the max.
    // If it is not an exact multiple of min, the RM will allocate to the
    // nearest multiple of min
    if (containerMemory < minMem) {
      log.info("Container memory specified below min threshold of cluster."
                   + " Using min value." + ", specified=" + containerMemory + ", min="
                   + minMem);
      containerMemory = minMem;
    } else if (containerMemory > maxMem) {
      log.info("Container memory specified above max threshold of cluster."
                   + " Using max value." + ", specified=" + containerMemory + ", max="
                   + maxMem);
      containerMemory = maxMem;
    }
  }

  public getProxy(Class protocol, InetSocketAddress addr) {
    rpc.getProxy(protocol, addr, config);
  }

  /**
   * Register self as a server
   * @return the new server
   */
  private Server startAMActionsServer() {
    server = new RPC.Builder(config)
        .setProtocol(HoyaAppMasterProtocol.class)
        .setInstance(this)
//        .setBindAddress(ADDRESS)
        .setPort(0)
        .setNumHandlers(5)
        .setVerbose(serviceArgs.xTest)
//        .setSecretManager(sm)
        .build();
    server.start();
    
    server
  }
  
  /**
   * Setup the request that will be sent to the RM for the container ask.
   *
   * @param numContainers Containers to ask for from RM
   * @return the setup ResourceRequest to be sent to RM
   */
  private AMRMClient.ContainerRequest setupContainerAskForRM(int numContainers) {
    // setup requirements for hosts
    // using * as any host initially
    String[] hosts = null
    String[] racks = null
    Priority pri = Records.newRecord(Priority.class);
    // TODO - what is the range for priority? how to decide?
    pri.priority = requestPriority;

    // Set up resource type requirements
    // For now, only memory is supported so we set memory requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.memory = containerMemory;

    AMRMClient.ContainerRequest request
    request = new AMRMClient.ContainerRequest(capability,
                                              hosts,
                                              racks,
                                              pri,
                                              numContainers);
    log.info("Requested container ask: $request");
    return request;
  }

  /**
   * Callback event when a container is allocated
   * @param allocatedContainers list of containers
   */
  @Override //AMRMClientAsync
  public void onContainersAllocated(List<Container> allocatedContainers) {
    log.info("Got response from RM for container ask, allocatedCnt="
                 + allocatedContainers.size());
    numAllocatedContainers.addAndGet(allocatedContainers.size());
    allocatedContainers.each { Container container ->
      log.info("Launching shell command on a new container.," +
               " containerId=$container.id," +
               " containerNode=$container.nodeId.host:$container.nodeId.port," +
               " containerNodeURI=$container.nodeHttpAddress," +
               " containerResourceMemory$container.resource.memory");
      // + ", containerToken"
      // +container.getContainerToken().getIdentifier().toString());

      HoyaRegionServiceLauncher launcher =
        new HoyaRegionServiceLauncher(this, container)
      Thread launchThread = new Thread(launcherThreadGroup,
               launcher,
               "container-${container.nodeId.host}:${container.nodeId.port},");

      // launch and start the container on a separate thread to keep
      // the main thread unblocked
      // as all containers may not be allocated at one go.
      synchronized (launchThreads) {
        launchThreads.add(launchThread);
      }
      launchThread.start();
      
    }

  }

  @Override //AMRMClientAsync
  public void onContainersCompleted(List<ContainerStatus> completedContainers) {
    log.info("Got response from RM for container ask, completedCnt="
                 + completedContainers.size());
    for (ContainerStatus status : completedContainers) {
      log.info("Got container status for" +
               " containerID=$status.containerId," +
               " state=$status.state," +
               " exitStatus=$status.exitStatus," +
               " diagnostics=$status.diagnostics");

      // non complete containers should not be here
      assert (status.state == ContainerState.COMPLETE);

      // increment counters for completed/failed containers
      int exitStatus = status.exitStatus;
      if (0 != exitStatus) {
        // container failed
        if (ContainerExitStatus.ABORTED != exitStatus) {
          // exec failed
          // counts as completed
          numCompletedContainers.incrementAndGet();
          numFailedContainers.incrementAndGet();
        } else {
          // container was killed by framework, possibly preempted
          // we should re-try as the container was lost for some reason
          numAllocatedContainers.decrementAndGet();
          numRequestedContainers.decrementAndGet();
          // we do not need to release the container as it would be done
          // by the RM
        }
      } else {
        // nothing to do
        // container completed successfully
        numCompletedContainers.incrementAndGet();
        log.info("Container completed successfully.," +
                 " containerId=$status.containerId");
      }
    }

    // ask for more containers if any failed
    int askCount = numTotalContainers - numRequestedContainers.get();
    numRequestedContainers.addAndGet(askCount);

    if (askCount > 0) {
      AMRMClient.ContainerRequest containerAsk = setupContainerAskForRM(askCount);
      asyncRMClient.addContainerRequest(containerAsk);
    }

    // set progress to deliver to RM on next heartbeat
    float progress = (float) numCompletedContainers.get() / numTotalContainers;
//    resourceManager.setProgress(progress);

    if (numCompletedContainers.get() == numTotalContainers) {
      signalAMComplete();
    }
  }

  /**
   * Update the cluster description with anything interesting
   */
  private synchronized void updateClusterDescription() {
    if (hbaseMaster) {
      masterNode.command = hbaseMaster.commands.join(" ");
      masterNode.state = hbaseMaster.running ?
        ClusterDescription.STATE_STARTED: ClusterDescription.STATE_STOPPED 
      //pull in recent lines of output from the HBase master
      List<String> output = hbaseMaster.recentOutput
      masterNode.output = output.toArray(new String[output.size()])
    } else {
      masterNode.state = ClusterDescription.STATE_UNSTARTED
      masterNode.output = new String[0];
    }
  }
  
  
  /**
   * RM wants to shut down the AM
   */
  @Override //AMRMClientAsync
  void onShutdownRequest() {
    log.info("Shutdown requested")
    signalAMComplete();
  }
  
/**
   * Monitored nodes have been changed
   * @param updatedNodes list of updated notes
   */
  @Override //AMRMClientAsync
  public void onNodesUpdated(List<NodeReport> updatedNodes) {
    log.info("Nodes updated: " +
             (updatedNodes*.getNodeId()).join(" "))
  }
/*
  @Override //AMRMClientAsync
  public float getProgress() {
    float f = progressCounter;
    progressCounter +=10.0f
    log.info("AMRMClientAsync.getProgress()=$f")
    if (progressCounter>=100) {
      log.info("Completed")
      done = true;
    }
    return f
  }*/

  /**
   * Use this as a generic heartbeater: 
   * 0 = not started, 50 = live, 100 = finished
   * @return
   */
  @Override //AMRMClientAsync
  public float getProgress() {
    if (serviceArgs.xNoMaster) {
      return 25f;
    }
    if (!hbaseMaster) {
      return 0f;
    }
    //hbase is running or finished
    if (hbaseMaster.running) {
      return 50f;
    } else {
      signalAMComplete();
      return 100f;
    }
  }

  @Override //AMRMClientAsync
  public void onError(Exception e) {
    //callback says it's time to finish
    log.error("AMRMClientAsync.onError() received $e",e)
    signalAMComplete();
  }

  @Override
  void stopCluster() throws IOException {
    log.info("HoyaAppMasterApi.stopCluster()")
    signalAMComplete();
  }

  @Override   //HoyaAppMasterApi
  void addNodes(int nodes) throws IOException {
    log.info("HoyaAppMasterApi.addNodes($nodes)")
  }

  @Override   //HoyaAppMasterApi
  void deleteNodes(int nodes) throws IOException {
    log.info("HoyaAppMasterApi.rmNodes($nodes)")
  }

  @Override   //HoyaAppMasterApi
  long getProtocolVersion(String protocol, long clientVersion) throws IOException {
    return versionID
  }

  @Override //HoyaAppMasterApi
  public synchronized String getClusterStatus() throws IOException {
    updateClusterDescription()
    String status = clusterDescription.toJsonString()
//    log.debug(status)
    return status; 
  }

  @Override   //HoyaAppMasterApi
  ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(
        this, protocol, clientVersion, clientMethodsHash);
  }


  protected synchronized void launchHBaseServer(List<String> commands, Map<String, String> env) throws IOException {
    if (hbaseMaster != null) {
      throw new HoyaInternalStateException("trying to launch hbase server" +
                                           " when it is already running")
    }
    commands.add(0, buildHBaseBinPath().absolutePath);
    hbaseMaster = new RunLongLivedApp(commands);
    hbaseMaster.putEnvMap(env);
    //set the env variable mapping
    hbaseMaster.putEnvMap(buildEnvMapFromServiceArguments())
    hbaseMaster.spawnApplication()
  }

  /**
   * Get the path to hbase home
   * @return the hbase home path
   */
  public File buildHBaseBinPath() {
    File hbaseScript = new File(serviceArgs.hbasehome,
                                "bin/hbase");
    return hbaseScript;
  }

  /**
   * Build the environment map from service arguments passed down
   * @return an env map to merge with the rest of the envirionment
   */
  public Map<String, String> buildEnvMapFromServiceArguments() {
    return [
        (EnvMappings.ENV_FS_DEFAULT_NAME):      serviceArgs.filesystem,
        (EnvMappings.ENV_ZOOKEEPER_CONNECTION): serviceArgs.zookeeper,
        (EnvMappings.ENV_ZOOKEEPER_PATH):       serviceArgs.hbasezkpath,
    ]
  }

  /**
   * stop hbase process if it the running process var is not null
   */
  protected synchronized void stopHBase() {
    hbaseMaster?.stop();
    //nullidy the variable
    hbaseMaster = null;
  }
}