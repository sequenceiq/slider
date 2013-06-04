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

import groovy.util.logging.Commons
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hoya.HoyaApp
import org.apache.hadoop.hoya.HoyaExitCodes
import org.apache.hadoop.hoya.api.HoyaAppMasterActions
import org.apache.hadoop.hoya.exceptions.HoyaException
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
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.exceptions.YarnException
import org.apache.hadoop.yarn.ipc.YarnRPC
import org.apache.hadoop.yarn.service.CompositeService
import org.apache.hadoop.yarn.service.launcher.RunService
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.hadoop.yarn.util.Records

import java.util.concurrent.atomic.AtomicInteger

/**
 * The AM for Hoya
 */
@Commons

class HoyaAppMaster extends CompositeService
    implements AMRMClientAsync.CallbackHandler,
      RunService,
      HoyaExitCodes,
      HoyaAppMasterActions {

  // YARN RPC to communicate with the Resource Manager or Node Manager
  private static final boolean VERBOSE_RPC = false
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
  private int numTotalContainers = 0;
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

  // Launch threads
  private final List<Thread> launchThreads = new ArrayList<Thread>();
  private volatile boolean done;
  private volatile boolean success;

  String[] argv
  private HoyaMasterServiceArgs serviceArgs;


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
    String rmAddress = serviceArgs.rmAddress
    YarnUtils.setRmAddress(conf, rmAddress)
    YarnUtils.setRmAddressGlobal(rmAddress);
    super.init(conf)
  }
/**
   * Just before the configuration is set, the args-supplied config is set
   * This is a way to sneak in config changes without subclassing init()
   * (so work with pre/post YARN-117 code)
   * @param conf new configuration.
   */
  @Override
  protected void setConfig(Configuration conf) {
    serviceArgs.applyDefinitions(conf);
    super.setConfig(conf)
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
   * Actual work
   * @return exit code
   * @throws Throwable on a failure
   */
  public int createAndRunCluster(String clustername) throws Throwable {
    YarnConfiguration conf = new YarnConfiguration(config);

    InetSocketAddress address = YarnUtils.getRmAddress(conf)
    log.info("RM is at $address")
    rpc = YarnRPC.create(conf);
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

    //add the RM client -this brings the callbacks in
    asyncRMClient = new AMRMClientAsync(appAttemptID, heartbeatInterval, this);
    //add to the list of things to terminate
    addService(asyncRMClient)
    //now bring it up
    asyncRMClient.init(conf);
    asyncRMClient.start();
    
    //set up the hostname & port details
    //initially: blank
    appMasterHostname = "localhost";
    appMasterRpcPort = 22;
    appMasterTrackingUrl = null;

    // Setup local RPC Server to accept status requests directly from clients
    // TODO need to setup a protocol for client to be able to communicate to
    // the RPC server
    // TODO use the rpc port info to register with the RM for the client to
    // send requests to this app master

    // Register self with ResourceManager
    // This will start heartbeating to the RM
    address = YarnUtils.getRmAddress(asyncRMClient.config)
    log.info("Connecting to RM at $address")
    RegisterApplicationMasterResponse response = asyncRMClient
        .registerApplicationMaster(appMasterHostname,
                                   appMasterRpcPort,
                                   appMasterTrackingUrl);
    configureContainerMemory(response)

    // Setup ask for containers from RM
    // Send request for containers to RM
    // Until we get our fully allocated quota, we keep on polling RM for
    // containers
    // Keep looping until all the containers are launched and shell script
    // executed on them ( regardless of success/failure).
    AMRMClient.ContainerRequest containerAsk = setupContainerAskForRM(numTotalContainers);
    asyncRMClient.addContainerRequest(containerAsk);
    numRequestedContainers.set(numTotalContainers);

    while (!done) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException ignored) {}
    }
    finish();

    return success ? EXIT_SUCCESS : EXIT_TASK_LAUNCH_FAILURE;
  }

  private void finish() {
    // Join all launched threads
    // needed for when we time out
    // and we need to release containers
    for (Thread launchThread : launchThreads) {
      try {
        launchThread.join(10000);
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
    } else {
      appStatus = FinalApplicationStatus.FAILED;
      appMessage = "Diagnostics., total=$numTotalContainers," +
                   " completed=${numCompletedContainers.get()}," +
                   " allocated=${numAllocatedContainers.get()}," +
                   " failed=${numFailedContainers.get()}";
      success = false;
    }
    try {
      log.info("Unregistering AM")
      asyncRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
    } catch (YarnException e) {
      log.error("Failed to unregister application: $e", e);
    } catch (IOException e) {
      log.error("Failed to unregister application: $e", e);
    }
    server?.stop()
    done = true;
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
        .setProtocol(HoyaAppMasterActions.class)
        .setInstance(this)
//        .setBindAddress(ADDRESS)
        .setPort(0)
        .setNumHandlers(5)
        .setVerbose(true)
//        .setSecretManager(sm)
        .build();
    server.start();
    InetSocketAddress address = NetUtils.getConnectAddress(server);
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
    pri.setPriority(requestPriority);

    // Set up resource type requirements
    // For now, only memory is supported so we set memory requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(containerMemory);

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
  @Override
  public void onContainersAllocated(List<Container> allocatedContainers) {
    log.info("Got response from RM for container ask, allocatedCnt="
                 + allocatedContainers.size());
    numAllocatedContainers.addAndGet(allocatedContainers.size());
    allocatedContainers.each { cont ->
      Container container = (Container) cont;
      log.info("Launching shell command on a new container.," +
               " containerId=$container.id," +
               " containerNode=$container.nodeId.host:$container.nodeId.port," +
               " containerNodeURI=$container.nodeHttpAddress," +
               " containerResourceMemory$container.resource.memory");
      // + ", containerToken"
      // +container.getContainerToken().getIdentifier().toString());

      HoyaRegionServiceLauncher launcher =
        new HoyaRegionServiceLauncher(this, container)
      Thread launchThread = new Thread(launcher);

      // launch and start the container on a separate thread to keep
      // the main thread unblocked
      // as all containers may not be allocated at one go.
      launchThreads.add(launchThread);
      launchThread.start();
    }

  }

  @Override
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
      done = true;
    }
  }

  /**
   * RM wants to reboot the AM
   */
  @Override
  public void onRebootRequest() {
    log.info("Reboot requested")
  }

  /**
   * Monitored nodes have been changed
   * @param updatedNodes list of updated notes
   */
  @Override
  public void onNodesUpdated(List<NodeReport> updatedNodes) {
    log.info("Nodes updated: " +
             (updatedNodes*.getNodeId()).join(" "))

  }

  @Override
  public float getProgress() {
    return 0.50f
  }

  @Override
  public void onError(Exception e) {
    //callback says it's time to finish
    log.error("onError received $e",e)
    done = true;
  }

  @Override
  void stopCluster() throws IOException {

  }

  @Override
  void addNodes(int nodes) throws IOException {

  }

  @Override
  void rmNodes(int nodes) throws IOException {

  }

  @Override
  long getProtocolVersion(String protocol, long clientVersion) throws IOException {
    return 0
  }

  @Override
  ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
    return null
  }
}