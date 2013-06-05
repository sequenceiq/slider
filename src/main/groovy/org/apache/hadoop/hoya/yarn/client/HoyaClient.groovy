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
import groovy.transform.CompileStatic
import groovy.util.logging.Commons
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem as FS
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hoya.HoyaApp
import org.apache.hadoop.hoya.HoyaExitCodes
import org.apache.hadoop.hoya.exceptions.BadConfigException
import org.apache.hadoop.hoya.exceptions.HoyaException
import org.apache.hadoop.hoya.tools.Duration
import org.apache.hadoop.hoya.tools.HoyaUtils
import org.apache.hadoop.hoya.tools.YarnUtils
import org.apache.hadoop.hoya.yarn.ZKIntegration
import org.apache.hadoop.hoya.yarn.appmaster.HoyaMasterServiceArgs
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse
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
import org.apache.hadoop.yarn.client.YarnClientImpl
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.exceptions.YarnException
import org.apache.hadoop.yarn.service.launcher.RunService
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.apache.hadoop.yarn.util.Records

import java.nio.ByteBuffer

/**
 * Client service for Hoya
 */
@Commons
@CompileStatic

class HoyaClient extends YarnClientImpl implements RunService, HoyaExitCodes {
  // App master priority
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

  @Override
  public String getName() {
    return "HoyaClient"
  }

  @Override
  public void setArgs(String...args) {
    this.argv = args;
    serviceArgs = new ClientArgs(args)
    serviceArgs.parse()
    serviceArgs.postProcess()
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
   * @throws Throwable anything that went wrong
   */
  @Override
  public int runService() throws Throwable {

    //choose the action
    String action = serviceArgs.action
    List<String> actionArgs = serviceArgs.actionArgs
    int exitCode = EXIT_SUCCESS
    switch(action) {
      
      case ClientArgs.ACTION_HELP:
        log.info("HoyaClient" + serviceArgs.usage())
        break;
      
      case ClientArgs.ACTION_CREATE:
        exitCode = createAM(actionArgs[0])
        break;
      
      case ClientArgs.ACTION_START:
        //throw new HoyaException("Start: " + actionArgs[0])

      default:
        throw new HoyaException(EXIT_UNIMPLEMENTED,
                               "Unimplemented: " + action)
    }

    return exitCode
  }

  
  /**
   * Create the AM
   */
  private int createAM(String clustername) {
    verifyValidClusterSize(serviceArgs.min)
    
    log.info("Setting up application submission context for ASM");
    ApplicationSubmissionContext appContext =
      Records.newRecord(ApplicationSubmissionContext.class);
    GetNewApplicationResponse newApp = super.getNewApplication();
    ApplicationId appId = newApp.applicationId
    // set the application id 
    appContext.applicationId = appId;
    // set the application name
    String appName = appName()
    appContext.applicationName = appName;
    String zkPath = ZKIntegration.mkClusterPath(getUsername(), clustername)

    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer =
      Records.newRecord(ContainerLaunchContext.class);

    // set local resources for the application master
    // local files or archives as needed
    // In this scenario, the jar file for the application master is part of the local resources			
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();


    if (!usingMiniMRCluster) {

      log.info("Copying JARs from local filesystem and add to local environment");
      // Copy the application master jar to the filesystem 
      // Create a local resource to point to the destination jar path 
      String subdir = "";
      String appPath = "$appName/${appId.id}/"
      //add this class
      localResources["hoya.jar"] = submitJarWithClass(this.class, appPath, subdir, "hoya.jar")
      //add lib classes that don't come automatically with YARN AM classpath
      String libdir = "lib/"
      localResources["groovayll.jar"] = submitJarWithClass(GroovyObject.class,
                                                           appPath,
                                                           libdir,
                                                           "groovayll.jar")

      localResources["jcommander.jar"] = submitJarWithClass(JCommander.class,
                                                            appPath,
                                                            libdir,
                                                            "jcommander.jar")
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
    // The shell script has to be made available on the final container(s)
    // where it will be executed. 
    // To do this, we need to first copy into the filesystem that is visible 
    // to the yarn framework. 
    // We do not need to set this as a local resource for the application 
    // master as the application master does not need it. 		
/*
    String hdfsShellScriptLocation = "";
    long hdfsShellScriptLen = 0;
    long hdfsShellScriptTimestamp = 0;
    if (!shellScriptPath.isEmpty()) {
      Path shellSrc = new Path(shellScriptPath);
      String shellPathSuffix = appName + "/" + appId.getId() + "/ExecShellScript.sh";
      Path shellDst = new Path(fs.getHomeDirectory(), shellPathSuffix);
      fs.copyFromLocalFile(false, true, shellSrc, shellDst);
      hdfsShellScriptLocation = shellDst.toUri().toString();
      FileStatus shellFileStatus = fs.getFileStatus(shellDst);
      hdfsShellScriptLen = shellFileStatus.getLen();
      hdfsShellScriptTimestamp = shellFileStatus.getModificationTime();
    }
*/

    // Set local resource info into app master container launch context
    amContainer.localResources = localResources;
    def env = [:]

    env['CLASSPATH'] = buildClasspath()

    amContainer.environment = env;

    String rmAddr = NetUtils.getHostPortString(YarnUtils.getRmSchedulerAddress(config))

    //build up the args list, intially as anyting
    List commands = []
    commands << ApplicationConstants.Environment.JAVA_HOME.$() + "/bin/java"
    commands << ServiceLauncher.ENTRY_POINT
    commands << HoyaMasterServiceArgs.CLASSNAME
    //now the app specific args
    commands << HoyaMasterServiceArgs.ARG_DEBUG
    commands << HoyaMasterServiceArgs.ACTION_CREATE
    commands << clustername
    commands << HoyaMasterServiceArgs.ARG_MIN
    commands << (Integer)serviceArgs.min
    commands << HoyaMasterServiceArgs.ARG_MAX
    commands << (Integer)serviceArgs.max
    
    //spec out the RM address
    commands << HoyaMasterServiceArgs.ARG_RM_ADDR;
    commands << rmAddr;
        
    //zk details -HBASE needs fs.default.name
    //hbase needs path inside ZK; skip ZK connect
    // use env variables & have that picked up and template it. ${env.SYZ}
    if (serviceArgs.zookeeper) {
      commands << HoyaMasterServiceArgs.ARG_ZOOKEEPER
      commands << serviceArgs.zookeeper
    }
    commands << HoyaMasterServiceArgs.ARG_ZK_PATH
    commands << zkPath
    
    
    //path in FS can be unqualified
    commands << HoyaMasterServiceArgs.ARG_PATH
    commands << "services/hoya/"
    commands << "1>${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/out.txt";
    commands << "2>${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/err.txt";
    StringBuilder cmd = new StringBuilder();

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
    int exitCode = EXIT_SUCCESS

    if (serviceArgs.waittime != 0) {
      //waiting for state to change
      Duration duration = new Duration(serviceArgs.waittime)
      duration.start()
      exitCode = monitorApplication(applicationId, duration)
    }
    return exitCode
  }

  private String getUsername() {
    return serviceArgs.user
  }

  private LocalResource submitJarWithClass(Class clazz, String appPath, String subdir, String jarName) {
    File localFile = HoyaUtils.findContainingJar(clazz);
    if (!localFile) {
      throw new HoyaException("Could not find JAR containing "
                                                 + clazz);
    }
    LocalResource resource = submitFile(localFile, appPath, subdir, jarName)
    resource
  }

  private LocalResource submitFile(File localFile, String appPath, String subdir, String destFileName) {
    FS hdfs = clusterFS;
    Path src = new Path(localFile.toString());
    String pathSuffix = appPath + "${subdir}$destFileName";
    Path destPath = new Path(hdfs.homeDirectory, pathSuffix);

    hdfs.copyFromLocalFile(false, true, src, destPath);

    // Set the type of resource - file or archive
    // archives are untarred at destination
    // we don't need the jar file to be untarred for now
    LocalResource resource = YarnUtils.createAmResource(hdfs,
                                                        destPath,
                                                        LocalResourceType.FILE)
    resource
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
  private FS getClusterFS() {
    FS.get(config)
  }

  /**
   * Verify that there are enough nodes in the cluster
   * @param requiredNumber required # of nodes
   * @throws BadConfigException if the config is wrong
   */
  private void verifyValidClusterSize(int requiredNumber) {
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

  private boolean getUsingMiniMRCluster() {
    return config.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)
  }

  private String appName() {
    "hoya"
  }

/**
 * Monitor the submitted application for completion. 
 * Kill application if time expires. 
 * @param appId Application Id of application to be monitored
 * @return true if application completed successfully
 * @throws YarnException
 * @throws IOException
 */
  private int monitorApplication(ApplicationId appId,
      Duration duration)
  throws YarnException, IOException {

    while (true) {

      // Check app status every 1 second.
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ignored) {
        log.debug("Thread sleep in monitoring loop interrupted");
      }

      // Get application report for the appId we are interested in 
      ApplicationReport report = getApplicationReport(appId);

      log.info("Got application report from ASM for, appId=${report.applicationId.id}, clientToken=${report.clientToken}, appDiagnostics=${report.diagnostics}, appMasterHost=${report.host}, appQueue=${report.queue}, appMasterRpcPort=${report.rpcPort}, appStartTime=${report.startTime}, yarnAppState=${report.yarnApplicationState}, distributedFinalState=${report.finalApplicationStatus}, appTrackingUrl=${report.trackingUrl}, appUser=${report.user}");

      YarnApplicationState state = report.yarnApplicationState;
      FinalApplicationStatus dsStatus = report.finalApplicationStatus;
      if (YarnApplicationState.FINISHED == state) {
        if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
          log.info("Application has completed successfully");
          return EXIT_SUCCESS;
        } else {
          log.info("Application finished unsuccessfully." +
                   " YarnState=" + state + ", DSFinalStatus=" + dsStatus +
                   ". Breaking monitoring loop");
          return EXIT_YARN_SERVICE_FINISHED_WITH_ERROR;
        }
      } else if (YarnApplicationState.KILLED == state ) {
        log.info("Application did not finish. YarnState=$state, DSFinalStatus=$dsStatus");
        return EXIT_YARN_SERVICE_KILLED;
      } else if (YarnApplicationState.FAILED == state) {
        log.info("Application Failed. YarnState=$state, DSFinalStatus=$dsStatus");
        return EXIT_YARN_SERVICE_FAILED;
      }

        if (duration.limitExceeded) {
        log.info("Reached client specified timeout for application. Killing application");
        forceKillApplication(appId);
        return EXIT_TIMED_OUT;
      }
    }

  }

  /**
   * Kill a submitted application by sending a call to the ASM
   * @param appId Application Id to be killed. 
   * @throws YarnException
   * @throws IOException
   */
  private void forceKillApplication(ApplicationId appId)
  throws YarnException, IOException {
    // TODO clarify whether multiple jobs with the same app id can be submitted and be running at 
    // the same time. 
    // If yes, can we kill a particular attempt only?

    // Response can be ignored as it is non-null on success or 
    // throws an exception in case of failures
    super.killApplication(appId);
  }
}
