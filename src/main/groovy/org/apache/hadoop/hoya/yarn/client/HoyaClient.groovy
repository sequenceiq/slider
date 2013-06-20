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

package org.apache.hadoop.hoya.yarn.client

import com.beust.jcommander.JCommander
import com.google.common.annotations.VisibleForTesting
import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import groovy.util.logging.Commons
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileAlreadyExistsException
import org.apache.hadoop.fs.FileSystem as HadoopFS
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hoya.HoyaApp
import org.apache.hadoop.hoya.HoyaExitCodes
import org.apache.hadoop.hoya.HoyaKeys
import org.apache.hadoop.hoya.api.ClusterDescription
import org.apache.hadoop.hoya.api.ClusterNode
import org.apache.hadoop.hoya.api.HoyaAppMasterProtocol
import org.apache.hadoop.hoya.exceptions.BadCommandArgumentsException
import org.apache.hadoop.hoya.exceptions.BadConfigException
import org.apache.hadoop.hoya.exceptions.HoyaException
import org.apache.hadoop.hoya.tools.ConfigHelper
import org.apache.hadoop.hoya.tools.Duration
import org.apache.hadoop.hoya.tools.HoyaUtils
import org.apache.hadoop.hoya.tools.YarnUtils
import org.apache.hadoop.hoya.yarn.CommonArgs
import org.apache.hadoop.hoya.yarn.appmaster.EnvMappings
import org.apache.hadoop.hoya.yarn.appmaster.HoyaMasterServiceArgs
import org.apache.hadoop.ipc.RPC
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import org.apache.hadoop.yarn.api.records.LocalResource
import org.apache.hadoop.yarn.api.records.LocalResourceType
import org.apache.hadoop.yarn.api.records.Priority
import org.apache.hadoop.yarn.api.records.Resource
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.client.api.YarnClientApplication
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.exceptions.YarnException
import org.apache.hadoop.yarn.service.launcher.RunService
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.apache.hadoop.yarn.util.Records
import org.codehaus.jackson.JsonParseException

import java.nio.ByteBuffer

/**
 * Client service for Hoya
 */
@Commons
@CompileStatic

class HoyaClient extends YarnClientImpl implements RunService, HoyaExitCodes {
  // App master priority
  public static final int ACCEPT_TIME = 60000
  public static final String E_CLUSTER_RUNNING = "cluster already running"
  public static final String E_ALREADY_EXISTS = "already exists"
  public static final String E_MISSING_PATH = "Missing path "
  public static final String E_INCOMPLETE_CLUSTER_SPEC = "Cluster specification is marked as incomplete: "
  public static final String E_UNKNOWN_CLUSTER = "Unknown cluster "
  public static final String E_DESTROY_CREATE_RACE_CONDITION = "created while it was being destroyed"
  private int amPriority = 0;
  // Queue for App master
  private String amQueue = "default";
  // Amt. of memory resource to request for to run the App Master
  private int amMemory = 10;

  private String[] argv
  private ClientArgs serviceArgs
  public ApplicationId applicationId;

  /**
   * Entry point from the service launcher
   */
  HoyaClient() {
    super("HoyaClient", null)
    //any app-wide actions
    new HoyaApp("HoyaClient")
  }

  /**
   * Constructor that takes the command line arguments and parses them
   * via {@link RunService#setArgs(String [])}. That method 
   * MUST NOT be called afterwards.
   * @param args argument list to be treated as both raw and processed
   * arguments.
   */
  public HoyaClient(String...args) {
    setArgs(args)
  }

  @Override //Service
  public String getName() {
    return "Hoya"
  }

  @Override
  public void setArgs(String...args) {
    this.argv = args;
    serviceArgs = new ClientArgs(args)
    serviceArgs.parse()
    serviceArgs.postProcess()
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    serviceArgs.applyDefinitions(conf);
    serviceArgs.applyFileSystemURL(conf)
    super.serviceInit(conf)
  }

  /**
   * this is where the work is done.
   * @return the exit code
   * @throws Throwable anything that went wrong
   */
  @Override
  public int runService() throws Throwable {

    //choose the action
    String action = serviceArgs.action
    List<String> actionArgs = serviceArgs.actionArgs
    int exitCode = EXIT_SUCCESS
    String clusterName = serviceArgs.clusterName;
    //actions
    switch(action) {

      case ClientArgs.ACTION_CREATE:
        validateClusterName(clusterName)
        exitCode = actionCreate(clusterName)
        break;

      case ClientArgs.ACTION_DESTROY:
        validateClusterName(clusterName)
        exitCode = actionDestroy(clusterName)
        break;

      case CommonArgs.ACTION_EXISTS:
        validateClusterName(clusterName)
        exitCode = actionExists(clusterName)
        break;

      
      case CommonArgs.ACTION_GETCONF:
        validateClusterName(clusterName)
        File outfile = null;
        if (serviceArgs.output != null) {
          outfile = new File(serviceArgs.output)
        }
        exitCode = actionGetConf(clusterName,
                                 serviceArgs.format,
                                 outfile)
        break;

      case ClientArgs.ACTION_HELP:
        log.info("HoyaClient" + serviceArgs.usage())
        break;

      case CommonArgs.ACTION_LIST:
        if (clusterName != null) {
          validateClusterName(clusterName)
        }
        exitCode = actionList(clusterName)
        break;

      case ClientArgs.ACTION_START:
        exitCode = actionStart(clusterName);
        break;

      case ClientArgs.ACTION_STATUS:
        validateClusterName(clusterName)
        exitCode = actionStatus(clusterName);
        break;

      case ClientArgs.ACTION_STOP:
        validateClusterName(clusterName)
        exitCode = actionStop(clusterName, 0)
        break;

      default:
        throw new HoyaException(EXIT_UNIMPLEMENTED,
                                "Unimplemented: " + action)
    }
    return exitCode
  }

  protected void validateClusterName(String clustername) {
    if (!HoyaUtils.isClusternameValid(clustername)) {
      throw new BadCommandArgumentsException("Illegal cluster name: `$clustername`")
    }
  }

  /**
   * Destroy a cluster. There's two race conditions here
   * #1 the cluster is started between verifying that there are no live
   * clusters of that name.
   */
  public int actionDestroy(String clustername) {
    //verify that a live cluster isn't there
    validateClusterName(clustername)
    verifyFileSystemArgSet()
    verifyManagerSet()
    verifyNoLiveClusters(clustername)

    //create the directory path
    Path clusterDirectory = HoyaUtils.createHoyaClusterDirPath(clusterFS, clustername)
    //delete the directory
    clusterFS.delete(clusterDirectory, true)

    // detect any race leading to cluster creation during the check/destroy process
    // and report a problem.
    if (findAllLiveInstances(null, clustername).size() > 0) {
      throw new HoyaException(EXIT_BAD_CLUSTER_STATE,
                              clustername + ": "
                              + E_DESTROY_CREATE_RACE_CONDITION
                              + " :" + findAllLiveInstances(null, clustername)[0])
    }
    return EXIT_SUCCESS
  }
  
  /**
   * Restore a cluster
   */
  public int actionStart(String clustername) {
    //verify that a live cluster isn't there
    validateClusterName(clustername)
    verifyFileSystemArgSet()

    Path clusterDirectory = HoyaUtils.createHoyaClusterDirPath(clusterFS, clustername)
    Path clusterSpecPath = new Path(clusterDirectory, HoyaKeys.CLUSTER_SPECIFICATION_FILE);
    if (!clusterFS.exists(clusterSpecPath)) {
      log.debug("Missing cluster specification file $clusterSpecPath")
      throw new HoyaException(EXIT_UNKNOWN_HOYA_CLUSTER,
                                             E_UNKNOWN_CLUSTER + clustername +
        "\n (cluster definition not found at $clusterSpecPath)")
    }
    ClusterDescription clusterSpec = ClusterDescription.load(clusterFS, clusterSpecPath);
    //spec is loaded, just look at its state
    if (clusterSpec.state == ClusterDescription.STATE_INCOMPLETE) {
      throw new HoyaException(EXIT_BAD_CLUSTER_STATE,E_INCOMPLETE_CLUSTER_SPEC + clusterSpecPath)
    }
  
    //now see if it is actually running and bail out
    verifyManagerSet()
    verifyNoLiveClusters(clustername)

    return executeClusterCreation(clusterSpec);
  }
  
  /**
   * Create the cluster -saving the arguments to a specification file first
   */
  private int actionCreate(String clustername) {

    //check for arguments that are mandatory with this action

    verifyFileSystemArgSet()
    verifyManagerSet()

    if (serviceArgs.zkhosts == null) {
      throw new BadCommandArgumentsException("Required argument "
                                                 + CommonArgs.ARG_ZKQUORUM
                                                 + " missing")
    }
    if (!serviceArgs.confdir) {
      throw new BadCommandArgumentsException("Missing argument ${CommonArgs.ARG_CONFDIR}")
    }
    
    //verify that a live cluster isn't there
    verifyNoLiveClusters(clustername)
    
    //build up the initial cluster specification
    ClusterDescription clusterSpec = new ClusterDescription()
    clusterSpec.name = clustername;
    clusterSpec.state = ClusterDescription.STATE_INCOMPLETE;
    clusterSpec.createTime = System.currentTimeMillis();
    int workers = serviceArgs.workers
    int workerHeap = serviceArgs.workerHeap
    validateNodeAndHeapValues("worker",workers, workerHeap)
    clusterSpec.workers = workers
    clusterSpec.workerHeap = workerHeap;
    int masters = serviceArgs.masters
    int masterHeap = serviceArgs.masterHeap
    validateNodeAndHeapValues("master", masters, masterHeap)
    if (masters > 1) {
      throw new BadCommandArgumentsException("No more than one master is currently supported")
    }
    clusterSpec.masters = masters
    clusterSpec.masterHeap = masterHeap

    //set up the ZK binding
    String zookeeperRoot = serviceArgs.hbasezkpath
    if (serviceArgs.hbasezkpath == null) {
      zookeeperRoot = "/yarnapps_${appName}_${username}_${clustername}"
    }
    clusterSpec.zkPath = zookeeperRoot
    clusterSpec.zkPort = serviceArgs.zkport
    clusterSpec.zkHosts = serviceArgs.zkhosts

    //build up the paths in the DFS

    Path clusterDirectory = HoyaUtils.createHoyaClusterDirPath(clusterFS, clustername)
    Path origConfPath = new Path(clusterDirectory, HoyaKeys.ORIG_CONF_DIR_NAME);
    Path generatedConfPath = new Path(clusterDirectory, HoyaKeys.GENERATED_CONF_DIR_NAME);
    Path clusterSpecPath = new Path(clusterDirectory, HoyaKeys.CLUSTER_SPECIFICATION_FILE);
    clusterSpec.originConfigurationPath = origConfPath.toUri().toASCIIString()
    clusterSpec.generatedConfigurationPath = generatedConfPath.toUri().toASCIIString()
    //save the specification to get a lock on this cluster name
    try {
      clusterSpec.save(clusterFS, clusterSpecPath, false)
    } catch (FileAlreadyExistsException fae) {
      throw new HoyaException(EXIT_BAD_CLUSTER_STATE,
                              clustername + ": " + E_ALREADY_EXISTS + " :" + clusterSpecPath)
    } catch (IOException e) {
      //this is probably a file exists exception too, but include it in the trace just in case
      throw new HoyaException(EXIT_BAD_CLUSTER_STATE,
                              clustername + ": " + E_ALREADY_EXISTS + " :" + clusterSpecPath,
                              e)
    }

    //bulk copy
    //first the original from wherever to the DFS
    HoyaUtils.copyDirectory(config, serviceArgs.confdir, origConfPath)
    //then build up the generated path
    HoyaUtils.copyDirectory(config, origConfPath, generatedConfPath)

    //HBase
    Path hBaseRootPath = new Path(clusterDirectory, HoyaKeys.HBASE_DATA_DIR_NAME);

    log.debug("hBaseRootPath=$hBaseRootPath")
    clusterSpec.hbaseRootPath = hBaseRootPath.toUri().toString();

    //HBase home
    clusterSpec.hbaseHome = serviceArgs.hbasehome
    //explicit hbase command set
    clusterSpec.xHBaseMasterCommand = serviceArgs.xHBaseMasterCommand
  
    
    //check for debug mode
    if (serviceArgs.xTest) {
      clusterSpec.flags[CommonArgs.ARG_X_TEST] = "true";
    }
    
    //here the configuration is set up. Mark the 
    clusterSpec.state = ClusterDescription.STATE_SUBMITTED;
    clusterSpec.save(clusterFS, clusterSpecPath, true)
    //here is where all the work is done
   
    
    return executeClusterCreation(clusterSpec)
  }

  public void verifyFileSystemArgSet() {
    if (serviceArgs.filesystemURL == null) {
      throw new BadCommandArgumentsException("Required argument "
                                                 + CommonArgs.ARG_FILESYSTEM
                                                 + " missing")
    }
  }
  
  
  public void verifyManagerSet() {
    InetSocketAddress rmAddr = YarnUtils.getRmAddress(config)
    if (! YarnUtils.isAddressDefined(rmAddr)) {
      throw new BadCommandArgumentsException(
          "No valid Resource Manager adddress provided in the argument "
             + CommonArgs.ARG_MANAGER
             + " or the configuration property "
             + YarnConfiguration.RM_ADDRESS)
    }
  }
  
  /**
   * Create a cluster to the specification
   * @param clusterSpec cluster specification
   * @return the exit code from the operation
   */
 public int executeClusterCreation(ClusterDescription clusterSpec) {

   //verify that a live cluster isn't there
   final String clustername = clusterSpec.name
   validateClusterName(clustername)
   verifyNoLiveClusters(clustername)
   //make sure it is valid
   verifyValidClusterSize(clusterSpec.workers)

   Path genConfPath = createPathThatMustExist(clusterSpec.generatedConfigurationPath)
   Path origConfPath = createPathThatMustExist(clusterSpec.originConfigurationPath)
   
   YarnClientApplication application = createApplication()
   ApplicationSubmissionContext appContext = application.applicationSubmissionContext

   ApplicationId appId = appContext.applicationId
    // set the application name
    appContext.applicationName = clustername
    //app type used in service enum
    appContext.applicationType = HoyaKeys.APP_TYPE

    if (clusterSpec.flags[CommonArgs.ARG_X_TEST]) {
      //test flag set
      appContext.maxAppAttempts = 1
    }

    Path tempPath = HoyaUtils.createHoyaAppInstanceTempPath(clusterFS,
                                                            clustername,
                                                            appId.toString())

    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer =
      Records.newRecord(ContainerLaunchContext.class);

    // set local resources for the application master
    // local files or archives as needed
    // In this scenario, the jar file for the application master is part of the local resources			
    Map<String, LocalResource> localResources = [:]

    if (!usingMiniMRCluster) {
      //the assumption here is that minimr cluster => this is a test run
      //and the classpath can look after itself

      log.info("Copying JARs from local filesystem and add to local environment");
      // Copy the application master jar to the filesystem
      // Create a local resource to point to the destination jar path 
      String bindir = "";
      //add this class
      localResources["hoya.jar"] = submitJarWithClass(this.class,
                                                      tempPath,
                                                      bindir,
                                                      "hoya.jar")
      //add lib classes that don't come automatically with YARN AM classpath
      String libdir = bindir + "lib/"
      localResources["groovayll.jar"] = submitJarWithClass(GroovyObject.class,
                                                           tempPath,
                                                           libdir,
                                                           "groovayll.jar")
      
      localResources["jcommander.jar"] = submitJarWithClass(JCommander.class,
                                                            tempPath,
                                                            libdir,
                                                            "jcommander.jar")
/*
      localResources["ant.jar"] = submitJarWithClass(JCommander.class, tempPath,
                                                     libdir,
                                                     "ant.jar")
*/
/*
      localResources["hbase.jar"] = submitJarWithClass( HConstants.class, tempPath,
                                                     libdir,
                                                     "hbase.jar")
*/
    }

    //build up the configuration

    
    //now load the template configuration and build the site. Note that the 
    //original confdir (as on the localfs of the client) is passed.
    //TODO: will this work when clusters are restarted (would we need to 
   //use the original configuration when the cluster was first started..)
   Configuration templateConf = ConfigHelper.loadTemplateConfiguration(config,
                                                   origConfPath,
                                                   HoyaKeys.HBASE_TEMPLATE,
                                                   HoyaKeys.HBASE_TEMPLATE_RESOURCE)

   //construct the cluster configuration values
    Map<String, String> clusterConfMap = buildConfMapFromServiceArguments(clusterSpec);
    //merge them
    ConfigHelper.addConfigMap(templateConf, clusterConfMap)
    
    //dump them @info
    if (log.debugEnabled) {
      ConfigHelper.dumpConf(templateConf);
    }



    //save the -site.xml config to the visible-to-all DFS
    //that generatedConfPath is in
    //this is the path for the site configuration

    Path sitePath = ConfigHelper.generateConfig(config,
                                      templateConf,
                                      genConfPath,
                                      HoyaKeys.HBASE_SITE);
    
    log.debug("Saving the config to $sitePath")
    Map<String, LocalResource> confResources;
    confResources = YarnUtils.submitDirectory(clusterFS,
                                              genConfPath,
                                    HoyaKeys.PROPAGATED_CONF_DIR_NAME)
    localResources.putAll(confResources)
    if (log.isDebugEnabled()) {
      localResources.each { String key, LocalResource val ->
        log.debug("$key=${YarnUtils.stringify(val.resource)}")
      }
    }

    // Set the log4j properties if needed 
/*
    if (!log4jPropFile.isEmpty()) {
      Path log4jSrc = new Path(log4jPropFile);
      Path log4jDst = new Path(fs.getHomeDirectory(), "log4j.props");
      fs.copyFromLocalFile(false, true, log4jSrc, log4jDst);
      FileStatus log4jFileStatus = fs.getFileStatus(log4jDst);
      LocalResource log4jRsrc = Records.newRecord(LocalResource.class);
      log4jRsrc.setType(LocalResourceType.FILE);
      log4jRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
      log4jRsrc.setResource(ConverterUtils.getYarnUrlFromURI(log4jDst.toUri()));
      log4jRsrc.setTimestamp(log4jFileStatus.getModificationTime());
      log4jRsrc.setSize(log4jFileStatus.getLen());
      localResources.put("log4j.properties", log4jRsrc);
    }

*/
    
    // Set local resource info into app master container launch context
    amContainer.localResources = localResources;
    def env = [:]

    env['CLASSPATH'] = buildClasspath()

    amContainer.environment = env;

    String rmAddr = serviceArgs.rmAddress;
   //spec out the RM address
   if (!rmAddr && YarnUtils.isRmSchedulerAddressDefined(config)) {
     rmAddr = NetUtils.getHostPortString(YarnUtils.getRmSchedulerAddress(config))
   }
   
    //build up the args list, intially as anyting
    List commands = []
    commands << ApplicationConstants.Environment.JAVA_HOME.$() + "/bin/java"
    //insert any JVM options
    commands << HoyaKeys.JAVA_FORCE_IPV4;
    //add the generic sevice entry point
    commands << ServiceLauncher.ENTRY_POINT
    //immeiately followed by the classname
    commands << HoyaMasterServiceArgs.CLASSNAME
    //now the app specific args
    commands << HoyaMasterServiceArgs.ARG_DEBUG
    commands << HoyaMasterServiceArgs.ACTION_CREATE
    commands << clustername
    //min #of nodes
    commands << HoyaMasterServiceArgs.ARG_WORKERS
    commands << (Integer) clusterSpec.workers
    commands << HoyaMasterServiceArgs.ARG_WORKER_HEAP
    commands << (Integer) clusterSpec.workerHeap
    commands << HoyaMasterServiceArgs.ARG_MASTERS
    commands << (Integer)clusterSpec.masters
    commands << HoyaMasterServiceArgs.ARG_MASTER_HEAP
    commands << (Integer) clusterSpec.masterHeap
    

   if (rmAddr) {
     commands << HoyaMasterServiceArgs.ARG_RM_ADDR;
     commands << rmAddr;
   }
        
    //now conf dir path -fileset in the DFS
    commands << HoyaMasterServiceArgs.ARG_GENERATED_CONFDIR
    commands << clusterSpec.generatedConfigurationPath

    String hbaseHome = clusterSpec.hbaseHome
    if (hbaseHome) {
      //HBase home
      commands << HoyaMasterServiceArgs.ARG_HBASE_HOME
      commands << hbaseHome
    }
    String xHBaseMasterCommand = clusterSpec.xHBaseMasterCommand
    if (xHBaseMasterCommand) {
      //explicit hbase command set
      commands << CommonArgs.ARG_X_HBASE_MASTER_COMMAND 
      commands << xHBaseMasterCommand
    }
    if (clusterSpec.flags[CommonArgs.ARG_X_TEST]) {
      //test flag set
      commands << CommonArgs.ARG_X_TEST 
    }
    if (serviceArgs.filesystemURL) {
      commands << HoyaMasterServiceArgs.ARG_FILESYSTEM
      commands << serviceArgs.filesystemURL;
    }

    //path in FS can be unqualified
    commands << HoyaMasterServiceArgs.ARG_PATH
    commands << "services/hoya/"
    commands << "1>${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/out.txt";
    commands << "2>${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/err.txt";

    String cmdStr = commands.join(" ")
    log.info("Completed setting up app master command $cmdStr");
    //sanity check: no null entries are allowed
    commands.each { assert it !=null }
    //uses the star-dot operator to apply the tostring method to all elements
    //of the array, returnigna new array
    List<String> commandListStr = commands*.toString();
    
    amContainer.commands = commandListStr
    // Set up resource type requirements
    // For now, only memory is supported so we set memory requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.memory = amMemory;
    appContext.resource = capability;
    Map<String, ByteBuffer> serviceData = [:]
    // Service data is a binary blob that can be passed to the application
    // Not needed in this scenario
    amContainer.serviceData = serviceData;

    // The following are not required for launching an application master 
    // amContainer.setContainerId(containerId);

    appContext.AMContainerSpec = amContainer;

    // Set the priority for the application master
    Priority pri = Records.newRecord(Priority.class);
    // TODO - what is the range for priority? how to decide? 
    pri.priority = amPriority;
    appContext.priority = pri;

    // Set the queue to which this application is to be submitted in the RM
    appContext.queue = amQueue;

    // Submit the application to the applications manager
    // SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest);
    // Ignore the response as either a valid response object is returned on success 
    // or an exception thrown to denote some form of a failure
    log.info("Submitting application to ASM");

    //submit the application
    applicationId = submitApplication(appContext)

    int exitCode
    //wait for the submit state to be reached
    ApplicationReport report = monitorAppToState(new Duration(ACCEPT_TIME),
                                                 YarnApplicationState.ACCEPTED);
    
    //may have failed, so check that
    if (YarnUtils.hasAppFinished(report)) {
      exitCode = buildExitCode(appId, report)
    } else {
      //exit unless there is a wait
      exitCode = EXIT_SUCCESS

      if (serviceArgs.waittime != 0) {
        //waiting for state to change
        Duration duration = new Duration(serviceArgs.waittime* 1000)
        duration.start()
        report = monitorAppToState(duration,
                                   YarnApplicationState.RUNNING);
        if (report && report.yarnApplicationState == YarnApplicationState.RUNNING) {
          exitCode = EXIT_SUCCESS
        } else {
          killRunningApplication(appId);
          exitCode = buildExitCode(appId, report)
        }
      }
    }
    return exitCode
  }

  /**
   * Validate the node count and heap size values of a node class 
   * @param name node class name
   * @param count requested node count
   * @param heap requested heap size
   * @throws BadCommandArgumentsException if the values are out of range
   */
  public void validateNodeAndHeapValues(String name, int count, int heap) {
    if (count < 0) {
      throw new BadCommandArgumentsException("requested no of $name nodes is too low: $count")
    }
    
    if (heap < HoyaKeys.MIN_HEAP_SIZE) {
      throw new BadCommandArgumentsException("requested heap size of $name nodes is too low: $count")
    }
    
  }

  /**
   * Create a path that must exist in the cluster fs
   * @param uri uri to create
   * @return the path
   * @throws HoyaException if the path does not exist
   */
  public Path createPathThatMustExist(String uri) {
    Path path = new Path(uri)
    verifyPathExists(path)
    return path;
  }

  public void verifyPathExists(Path path) {
    if (!clusterFS.exists(path)) {
      throw new HoyaException(EXIT_BAD_CLUSTER_STATE,
                              E_MISSING_PATH + path)
    }
  }

  /**
   * verify that a live cluster isn't there
   * @param clustername cluster name
   * @throws HoyaException with exit code EXIT_BAD_CLUSTER_STATE
   * if a cluster of that name is either live or starting up.
   */
  public void verifyNoLiveClusters(String clustername) {
    Collection<ApplicationReport> existing = findAllLiveInstances(null, clustername)

    if (existing.size() > 0) {
      throw new HoyaException(EXIT_BAD_CLUSTER_STATE,
                clustername + ": " + E_CLUSTER_RUNNING + " :" + existing[0])
    }
  }

  public String getUsername() {
    return UserGroupInformation.getCurrentUser().getShortUserName();
  }

  /**
   * Submit a JAR containing a specific class.
   * @param clazz class to look for
   * @param appPath app path
   * @param subdir subdirectory  (expected to end in a "/")
   * @param jarName <i>At the destination</i>
   * @return the local resource ref
   * @throws IOException trouble copying to HDFS
   */
  private LocalResource submitJarWithClass(Class clazz, Path tempPath, String subdir, String jarName)
        throws IOException, HoyaException{
    File localFile = HoyaUtils.findContainingJar(clazz);
    if (!localFile) {
      throw new FileNotFoundException("Could not find JAR containing " + clazz);
    }
    LocalResource resource = submitFile(localFile, tempPath, subdir, jarName)
    return resource
  }

  /**
   * Submit a local file to the filesystem references by the instance's cluster
   * filesystem
   * @param localFile filename
   * @param clusterName application path
   * @param subdir subdirectory (expected to end in a "/")
   * @param destFileName destination filename
   * @return the local resource ref
   * @throws IOException trouble copying to HDFS
   */
  private LocalResource submitFile(File localFile, Path tempPath, String subdir, String destFileName) throws IOException {
    Path src = new Path(localFile.toString());
    Path destPath = new Path(tempPath, "${subdir}$destFileName")
    // new Path(hdfs.homeDirectory, subPath);

    clusterFS.copyFromLocalFile(false, true, src, destPath);

    // Set the type of resource - file or archive
    // archives are untarred at destination
    // we don't need the jar file to be untarred for now
    LocalResource resource = YarnUtils.createAmResource(clusterFS,
                               destPath,
                               LocalResourceType.FILE)
    return resource
  }

  /**
   * Create an AM resource from the 
   * @param hdfs HDFS or other filesystem in use
   * @param destPath dest path in filesystem
   * @param resourceType resource type
   * @return the resource set up wih application-level visibility and the
   * timestamp & size set from the file stats.
   */
  
  /**
   * Get the filesystem of this cluster
   * @return the FS of the config
   */
  private HadoopFS getClusterFS() {
    return HadoopFS.get(serviceArgs.filesystemURL, config)
  }

  /**
   * Verify that there are enough nodes in the cluster
   * @param requiredNumber required # of nodes
   * @throws BadConfigException if the config is wrong
   */
  private void verifyValidClusterSize(int requiredNumber) {
    if (requiredNumber == 0) {
      return
    }
    int nodeManagers = yarnClusterMetrics.numNodeManagers
    if (nodeManagers < requiredNumber) {
      throw new BadConfigException("Not enough nodes in the cluster:" +
                                   " need $requiredNumber" +
                                   " -but there are only $nodeManagers nodes");
    }
  }
  
  

  private String buildClasspath() {
// Add AppMaster.jar location to classpath
    // At some point we should not be required to add 
    // the hadoop specific classpaths to the env. 
    // It should be provided out of the box. 
    // For now setting all required classpaths including
    // the classpath to "." for the application jar
    StringBuilder classPathEnv = new StringBuilder()
    // add the runtime classpath needed for tests to work
    if (getUsingMiniMRCluster()) {
      //for mini cluster we pass down the java CP properties
      //and nothing else
      classPathEnv.append(System.getProperty("java.class.path"));
    } else {
      classPathEnv.append(ApplicationConstants.Environment.CLASSPATH.$())
          .append(File.pathSeparatorChar).append("./*");
      for (String c : config.getStrings(
          YarnConfiguration.YARN_APPLICATION_CLASSPATH,
          YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
        classPathEnv.append(File.pathSeparatorChar);
        classPathEnv.append(c.trim());
      }
      classPathEnv.append(File.pathSeparatorChar).append("./log4j.properties");
    }
    return classPathEnv.toString()
  }

  /**
   * ask if the client is using a mini MR cluster
   * @return
   */
  private boolean getUsingMiniMRCluster() {
    return config.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)
  }

  private String getAppName() {
    "hoya"
  }

  /**
   * Build the conf dir from the service arguments, adding the hbase root
   * to the FS root dir
   * @param hbaseRoot
   * @return a map of the dynamic bindings for this Hoya instance
   */
  @VisibleForTesting
  public Map<String, String> buildConfMapFromServiceArguments(ClusterDescription clusterSpec) {
    Map<String, String> envMap = [
        (EnvMappings.KEY_HBASE_MASTER_PORT): "0",
        (EnvMappings.KEY_HBASE_ROOTDIR): clusterSpec.hbaseRootPath,
        (EnvMappings.KEY_REGIONSERVER_INFO_PORT): "0",
        (EnvMappings.KEY_REGIONSERVER_PORT): "0",
        (EnvMappings.KEY_ZNODE_PARENT): clusterSpec.zkPath,
        (EnvMappings.KEY_ZOOKEEPER_PORT): clusterSpec.zkPort.toString(),
        (EnvMappings.KEY_ZOOKEEPER_QUORUM): clusterSpec.zkHosts,
    ]
    if (!getUsingMiniMRCluster()) {
      envMap.put("hbase.cluster.distributed", "true")
    }
    envMap
  }

  /**
   * Monitor the submitted application for reaching the requested state.
   * Will also report if the app reaches a later state (failed, killed, etc)
   * Kill application if duration!= null & time expires. 
   * @param appId Application Id of application to be monitored
   * @param duration how long to wait
   * @param desiredState desired state.
   * @return true if application completed successfully
   * @throws YarnException YARN or app issues
   * @throws IOException IO problems
   */
  @VisibleForTesting
  public int monitorAppToCompletion(Duration duration)
      throws YarnException, IOException {


    ApplicationReport report = monitorAppToState(duration,
                                       YarnApplicationState.FINISHED)

    return buildExitCode(applicationId, report)
  }

  /**
   * Wait for the app to start running (or go past that state)
   * @param duration time to wait
   * @return the app report; null if the duration turned out
   * @throws YarnException YARN or app issues
   * @throws IOException IO problems
   */
  @VisibleForTesting
  public ApplicationReport monitorAppToRunning(Duration duration)
      throws YarnException, IOException {
    return monitorAppToState(duration,
                             YarnApplicationState.RUNNING)

  }

  private boolean maybeKillApp(ApplicationReport report) {
    if (!report) {
      log.debug("Reached client specified timeout for application. Killing application");
      forceKillApplication();
    }
    return false;
  }
  /**
   * Build an exit code for an application Id and its report.
   * If the report parameter is null, the app is killed
   * @param appId app
   * @param report report
   * @return the exit code
   */
  private int buildExitCode(ApplicationId appId, ApplicationReport report) {
    if (!report) {
      log.info("Reached client specified timeout for application. Killing application");
      forceKillApplication();
      return EXIT_TIMED_OUT;
    }

    YarnApplicationState state = report.yarnApplicationState
    FinalApplicationStatus dsStatus = report.finalApplicationStatus;
    switch (state) {
      case YarnApplicationState.FINISHED:
        if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
          log.info("Application has completed successfully");
          return EXIT_SUCCESS;
        } else {
          log.info("Application finished unsuccessfully." +
                   " YarnState=" + state + ", DSFinalStatus=" + dsStatus +
                   ". Breaking monitoring loop");
          return EXIT_YARN_SERVICE_FINISHED_WITH_ERROR;
        }

      case YarnApplicationState.KILLED:
        log.info("Application did not finish. YarnState=$state, DSFinalStatus=$dsStatus");
        return EXIT_YARN_SERVICE_KILLED;

      case YarnApplicationState.FAILED:
        log.info("Application Failed. YarnState=$state, DSFinalStatus=$dsStatus");
        return EXIT_YARN_SERVICE_FAILED;
      default:
        //not in any of these states
        return EXIT_SUCCESS;
    }
  }
/**
 * Monitor the submitted application for reaching the requested state.
 * Will also report if the app reaches a later state (failed, killed, etc)
 * Kill application if duration!= null & time expires. 
 * @param appId Application Id of application to be monitored
 * @param duration how long to wait -must be more than 0
 * @param desiredState desired state.
 * @return the application report -null on a timeout
 * @throws YarnException
 * @throws IOException
 */
  @VisibleForTesting
  public ApplicationReport monitorAppToState(
      Duration duration, YarnApplicationState desiredState)
  throws YarnException, IOException {
    monitorAppToState(applicationId, desiredState, duration)
  }
/**
 * Monitor the submitted application for reaching the requested state.
 * Will also report if the app reaches a later state (failed, killed, etc)
 * Kill application if duration!= null & time expires. 
 * @param appId Application Id of application to be monitored
 * @param duration how long to wait -must be more than 0
 * @param desiredState desired state.
 * @return the application report -null on a timeout
 * @throws YarnException
 * @throws IOException
 */
  @VisibleForTesting
  public ApplicationReport monitorAppToState(
      ApplicationId appId, YarnApplicationState desiredState, Duration duration)
  throws YarnException, IOException {
    
    
    duration.start();
    if (duration.limit <= 0) {
      throw new HoyaException("Invalid duration of monitoring");
    }
    while (true) {

      // Get application report for the appId we are interested in 

      ApplicationReport report = getApplicationReport(appId);

      log.info("Got application report from ASM for, appId=${appId}, appDiagnostics=${report.diagnostics}, appMasterHost=${report.host}, appQueue=${report.queue}, appMasterRpcPort=${report.rpcPort}, appStartTime=${report.startTime}, yarnAppState=${report.yarnApplicationState}, distributedFinalState=${report.finalApplicationStatus}, appTrackingUrl=${report.trackingUrl}, appUser=${report.user}");

      YarnApplicationState state = report.yarnApplicationState;
      if (state >= desiredState) {
        log.debug("App in desired state (or higher) : $state")
        return report;
      }
      if (duration.limitExceeded) {
        return null;
      }

      // sleep 1s.
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ignored) {
        log.debug("Thread sleep in monitoring loop interrupted");
      }
    }
  }

  /**
   * Kill the submitted application by sending a call to the ASM
   * @throws YarnException
   * @throws IOException
   */
  public boolean forceKillApplication()
        throws YarnException, IOException {
    if (applicationId != null) {
      killRunningApplication(applicationId);
      return true;
    }
    return false;
  }

  /**
   * Kill a running application
   * @param applicationId
   * @return the response
   * @throws YarnException YARN problems
   * @throws IOException IO problems
   */
  private KillApplicationResponse killRunningApplication(ApplicationId applicationId) throws
      YarnException,
      IOException {
    log.info("Killing application " + applicationId);
    KillApplicationRequest request =
      Records.newRecord(KillApplicationRequest.class);
    request.setApplicationId(applicationId);
    return rmClient.forceKillApplication(request);
  }

  /**
   * List Hoya instances belonging to a specific user
   * @param user user: "" means all users
   * @return a possibly empty list of Hoya AMs
   */
  @VisibleForTesting
  public List<ApplicationReport> listHoyaInstances(String user)
    throws YarnException, IOException {
    List<ApplicationReport> allApps = applicationList;
    List<ApplicationReport> results = []
    allApps.each { ApplicationReport report ->
      if (   report.applicationType == HoyaKeys.APP_TYPE
          && (!user || user == report.user)) {
        results << report;
      }
    }
    return results;
  }

  /**
   * Implement the list action: list all nodes
   * @return exit code of 0 if a list was created
   */
  @VisibleForTesting
  public int actionList(String clustername) {
    verifyManagerSet()

    String user = serviceArgs.user
    List<ApplicationReport> instances = listHoyaInstances(user);

    if (!clustername) {
      log.info("Hoya instances for ${user ? user : 'all users'} : ${instances.size()} ");
      instances.each { ApplicationReport report ->
        logAppReport(report)
      }
      return EXIT_SUCCESS;
    } else {
      validateClusterName(clustername)
      log.debug("Listing cluster named $clustername")
      ApplicationReport report = findClusterInInstanceList(instances, clustername)
      if (report) {
        logAppReport(report)
        return EXIT_SUCCESS;
      } else {
        throw unknownClusterException(clustername)
      }
    }
  }

  public void logAppReport(ApplicationReport report) {
    log.info("Name        : ${report.name}")
    log.info("YARN status : ${report.yarnApplicationState}")
    log.info("Start Time  : ${report.startTime}")
    log.info("Finish Time : ${report.startTime}")
    log.info("RPC         : ${report.host}:${report.rpcPort}")
    log.info("Diagnostics : ${report.diagnostics}")
  }

  /**
   * Implement the islive action: probe for a cluster of the given name existing
   * 
   * @return exit code
   */
  @VisibleForTesting
  public int actionExists(String name) {
    verifyManagerSet()
    ApplicationReport instance = findInstance(getUsername(), name)
    if (!instance) {
      throw unknownClusterException(name)
    }
    return EXIT_SUCCESS;
  }

  @VisibleForTesting
  public ApplicationReport findInstance(String user, String appname) {
    List<ApplicationReport> instances = listHoyaInstances(user);
    return findClusterInInstanceList(instances, appname)
  }

  /**
   * find all instances of a specific app -if there is >1 in the cluster,
   * this returns them all
   * @param user user
   * @param appname application name
   * @return the list of all matching application instances
   */
  @VisibleForTesting
  public Collection<ApplicationReport> findAllInstances(String user, String appname) {
    List<ApplicationReport> instances = listHoyaInstances(user);
    return instances.findAll { ApplicationReport report ->
      report.name == appname
    }
  }

  /**
   * find all live instances of a specific app -if there is >1 in the cluster,
   * this returns them all. State should be running or less
   * @param user user
   * @param appname application name
   * @return the list of all matching application instances
   */
  @VisibleForTesting
  public Collection<ApplicationReport> findAllLiveInstances(String user, String appname) {
    List<ApplicationReport> instances = listHoyaInstances(user);
    return instances.findAll { ApplicationReport app ->
      app.name == appname &&
      app.yarnApplicationState <= YarnApplicationState.RUNNING
    }
  }

  public ApplicationReport findClusterInInstanceList(List<ApplicationReport> instances, String appname) {
    ApplicationReport found = null;
    ApplicationReport foundAndLive = null;
    
    instances.each { ApplicationReport report ->
      if (report.name == appname) {
        found = report;
        if (report.yarnApplicationState<=YarnApplicationState.RUNNING) {
          foundAndLive = report
        }
      }
    }
    if (foundAndLive) {
      found = foundAndLive
    }
    return found;
  }

  @VisibleForTesting
  public HoyaAppMasterProtocol connect(ApplicationReport report) {
    String host = report.host
    int port = report.rpcPort
    String address= report.host + ":" + port;
    if (!host || !port ) {
      throw new HoyaException(EXIT_CONNECTIVTY_PROBLEM,
                              "Hoya instance $report.name isn't" +
                              " providing a valid address for the" +
                              " Hoya RPC protocol: <$address>")
    }
    InetSocketAddress addr = NetUtils.createSocketAddrForHost(host, port);
    log.debug("Connecting to Hoya Server at " + addr);
    def protoProxy = RPC.getProtocolProxy(HoyaAppMasterProtocol,
                        HoyaAppMasterProtocol.versionID,
                        addr,
                        UserGroupInformation.getCurrentUser(),
                        getConfig(),
                        NetUtils.getDefaultSocketFactory(getConfig()),
                        15000,
                        null)
    HoyaAppMasterProtocol hoyaServer = protoProxy.proxy
    return hoyaServer;
  }
  /**
   * Status operation; 'name' arg defines cluster name.
   * @return
   */
  @VisibleForTesting
  public int actionStatus(String clustername) {
    verifyManagerSet()
    validateClusterName(clustername)
    ClusterDescription status = getClusterStatus(clustername)
    log.info(JsonOutput.prettyPrint(status.toJsonString()));
    return EXIT_SUCCESS
  }

  /**
   * Stop the cluster
   * @param clustername cluster name
   * @return the cluster name
   */
  public int actionStop(String clustername, int waittime) {
    verifyManagerSet()
    validateClusterName(clustername)
    ApplicationReport instance = findInstance(getUsername(), clustername)
    if (!instance) {
      //exit early
      return EXIT_SUCCESS;
    }
    HoyaAppMasterProtocol appMaster = connect(instance)
    appMaster.stopCluster();
    if (waittime > 0) {
      monitorAppToState(instance.applicationId,
                        YarnApplicationState.FINISHED,
                        new Duration(waittime))
    }
    return EXIT_SUCCESS
  }

  /**
   * get the cluster configuration
   * @param clustername cluster name
   * @return the cluster name
   */
  public int actionGetConf(String clustername, String format, File outputfile) {
    verifyManagerSet()
    validateClusterName(clustername)
    ClusterDescription status = getClusterStatus(clustername)
    Writer writer
    boolean toPrint
    if (outputfile != null) {
      writer = new FileWriter(outputfile)
      toPrint = false
    } else {
      writer = new StringWriter()
      toPrint = true
    }
    String description = "HBase cluster $clustername"
    //extract the config
    if ("xml" == format) {
      Configuration siteConf = new Configuration(false)
      status.hBaseClientProperties.each { String key, String val ->
        siteConf.set(key, val, description);
      }
      siteConf.writeXml(writer)
    } else if ("property" ==format) {
      Properties props = new Properties()
      status.hBaseClientProperties.each { String key, String val ->
        props[key] = val
      }
      props.store(writer, description)
    } else {
      throw new BadCommandArgumentsException("Unknown format: $format")
    }
    //data is written.
    //close the file
    writer.close();
    //then, if this is not a file write, print it
    if (toPrint) {
      System.out.println(writer.toString())
    } 
    return EXIT_SUCCESS
  }

  @VisibleForTesting
  public ClusterDescription getClusterStatus(String clustername) {
    HoyaAppMasterProtocol appMaster = bondToCluster(clustername)
    String statusJson = appMaster.getClusterStatus()
    try {
      ClusterDescription cd = ClusterDescription.fromJson(statusJson)
      return cd
    } catch (JsonParseException e) {
      log.error("Exception $e parsing:\n" + JsonOutput.prettyPrint(statusJson), e)
      throw e;
    }
  }
  
  private HoyaAppMasterProtocol bondToCluster(String clustername) {
    verifyManagerSet()
    ApplicationReport instance = findInstance(getUsername(), clustername)
    if (!instance) {
      throw unknownClusterException(clustername)
    }
    HoyaAppMasterProtocol appMaster = connect(instance);
    return appMaster
  }

  /**
   * Wait for the hbase master to be live (or past it in the lifecycle)
   * @param clustername cluster
   * @param timeout time to wait
   * @return the state. If still in CREATED, the cluster didn't come up
   * in the time period. If LIVE, all is well. If >LIVE, it has shut for a reason
   * @throws IOException
   * @throws HoyaException
   */
  public int waitForHBaseMasterLive(String clustername, long timeout)
      throws IOException, HoyaException {
    Duration duration = new Duration(timeout).start();
    boolean live = false;
    int state = ClusterDescription.STATE_CREATED
    while (!live) {
      ClusterDescription cd = getClusterStatus(clustername)
      //see if there is a master node yet
      if (cd.masterNodes.size() != 0) {
        //if there is, get the node
        ClusterNode master = cd.masterNodes[0];
        state = master.state
        live = state >= ClusterDescription.STATE_LIVE
        }
      if (!live && !duration.limitExceeded) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ignored) {
          //ignored
        }
      }
    }
    return state;
  }


  public HoyaException unknownClusterException(String clustername) {
    return new HoyaException(EXIT_UNKNOWN_HOYA_CLUSTER,
                            "Hoya cluster not found: '${clustername}' ")
  }

}
