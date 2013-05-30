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

package org.apache.hadoop.yarn.service.launcher;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.service.IrqHandler;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.service.ServiceOperations;
import org.apache.hadoop.yarn.service.ServiceStateException;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A class to launch any service by name.
 * It is assumed that the service starts 
 * 
 * Workflow
 * <ol>
 *   <li>An instance of the class is created</li>
 *   <li>It's service.init() and service.start() methods are called.</li>
 *   <li></li>
 * </ol>
 */
public class ServiceLauncher 
  implements LauncherExitCodes, IrqHandler.Interrupted {
  private static final Log LOG = LogFactory.getLog(ServiceLauncher.class);
  protected static final int PRIORITY = 30;

  /**
   * name of class for entry point strings: {@value}
   */
  public static final String ENTRY_POINT =
    "org.apache.hadoop.yarn.service.launcher.ServiceLauncher";


  public static final String USAGE_MESSAGE =
    "Usage: ServiceLauncher classname [--conf <conf file>] <service arguments> | ";

  /**
   * Name of the "--conf" argument. 
   */
  public static final String ARG_CONF = "--conf";
  static int SHUTDOWN_TIME_ON_INTERRUPT = 30 * 1000;

  private volatile Service service;
  private int serviceExitCode;
  private final List<IrqHandler> interruptHandlers =
    new ArrayList<IrqHandler>(1);
  private Configuration configuration;
  private String serviceClassName;

  /**
   * Create an instance of the launcher
   * @param serviceClassName classname of the service
   */
  public ServiceLauncher(String serviceClassName) {
    this.serviceClassName = serviceClassName;
  }

  /**
   * Get the service. Null until and unless
   * {@link #launchService(Configuration, String[], boolean)} has completed
   * @return
   */
  public Service getService() {
    return service;
  }

  /**
   * Get the configuration constructed from the command line arguments
   * @return the configuration used to create the service
   */
  public Configuration getConfiguration() {
    return configuration;
  }

  /**
   * The exit code from a successful service execution
   * @return the exit code. 
   */
  public int getServiceExitCode() {
    return serviceExitCode;
  }


  @Override
  public String toString() {
    return "ServiceLauncher for " + serviceClassName ;
  }

  /**
   * Launch the service, by creating it, initing it, starting it and then
   * maybe running it. {@link RunService#setArgs(String[])} is invoked
   * on the service between creation and init.
   * 
   * All exceptions that occur are propagated upwards.
   *
   * If the method returns a status code, it means that it got as far starting
   * the service, and if it implements {@link RunService}, that the 
   * method {@link RunService#runService()} has completed. 
   *
   * At this point, the service returned by {@link #get}
   *
   * @param conf configuration
   * @param processedArgs arguments after the configuration parameters
   * have been stripped out.
   * @param addShutdownHook should a shutdown hook be added to terminate
   * this service on shutdown. Tests should set this to false.
   * @throws ClassNotFoundException classname not on the classpath
   * @throws IllegalAccessException not allowed at the class
   * @throws InstantiationException not allowed to instantiate it
   * @throws InterruptedException thread interrupted
   * @throws IOException any IO exception
   */
  public int launchService(Configuration conf,
                           String[] processedArgs,
                           boolean addShutdownHook)
    throws Throwable {

    instantiateService(conf);

    //Register the interrupt handlers
    registerInterruptHandler();
    //and the shutdown hook
    if (addShutdownHook) {
      ServiceOperations.ServiceShutdownHook shutdownHook =
        new ServiceOperations.ServiceShutdownHook(service);
      ShutdownHookManager.get().addShutdownHook(
        shutdownHook, PRIORITY);
    }
    RunService runService = null;
    
    if (service instanceof RunService) {
      //if its a runService, pass in the arguments (hopefully before init)
      runService = (RunService) service;
      runService.setArgs(processedArgs);
    }
    
    //some class constructors init; here this is picked up on.
    if (!service.isInState(Service.STATE.INITED)) {
      service.init(configuration);
    }
    service.start();
    int exitCode = EXIT_SUCCESS;
    if (runService != null) {
      //assume that runnable services are meant to run from here
      exitCode = runService.runService();
    } else {
      //run the service until it stops or an interrupt happens on a different thread.
      service.waitForServiceToStop(0);
    }
    //exit
    serviceExitCode = exitCode;
    return serviceExitCode;
  }

  /**
   * Instantiate the service defined in <code>serviceClassName</code>
   * . Sets the <code>configuration</code> field
   * to the configuration, and <code>service</code> to the service.
   *
   * @param conf configuration to use
   * @throws ClassNotFoundException no such class
   * @throws InstantiationException no empty constructor,
   * problems with dependencies
   * @throws IllegalAccessException no access rights
   */
  public Service instantiateService(Configuration conf) throws
                                                     ClassNotFoundException,
                                                     InstantiationException,
                                                     IllegalAccessException {
    configuration = conf;

    //Instantiate the class -this requires the service to have a public
    // zero-argument constructor
    Class<?> serviceClass =
      this.getClass().getClassLoader().loadClass(serviceClassName);
    Object instance = serviceClass.newInstance();
    if (!(instance instanceof Service)) {
      //not a service
      throw new ServiceStateException("Not a Service: " + serviceClassName);
    }

    service = (Service) instance;
    return service;
  }

  /**
   * Register this class as the handler for the control-C interrupt.
   * Can be overridden for testing.
   * @throws IOException on a failure to add the handler
   */
  protected void registerInterruptHandler() throws IOException {
    interruptHandlers.add(new IrqHandler(IrqHandler.CONTROL_C, this));
  }

  /**
   * The service has been interrupted. 
   * Trigger something resembling an elegant shutdown;
   * Give the service time to do this before the exit operation is called 
   * @param interruptData the interrupted data.
   */
//  @Override
  public void interrupted(IrqHandler.InterruptData interruptData) {
    boolean controlC = IrqHandler.CONTROL_C.equals(interruptData.name);
    int shutdownTimeMillis = SHUTDOWN_TIME_ON_INTERRUPT;
    //start an async shutdown thread with a timeout
    ServiceForcedShutdown forcedShutdown =
      new ServiceForcedShutdown(shutdownTimeMillis);
    Thread thread = new Thread(forcedShutdown);
    thread.setDaemon(true);
    thread.start();
    //wait for that thread to finish
    try {
      thread.join(shutdownTimeMillis);
    } catch (InterruptedException ignored) {
      //ignored
    }
    if (!forcedShutdown.isServiceStopped()) {
      LOG.warn("Service did not shut down in time");
    }
    exit(controlC ? EXIT_SUCCESS : EXIT_INTERRUPTED);
  }

  /**
   * Exit the code.
   * This is method can be overridden for testing, throwing an 
   * exception instead. Any subclassed method MUST raise an 
   * {@link ExitUtil.ExitException} instance.
   * The service launcher code assumes that after this method is invoked,
   * no other code in the same method is called.
   * @param exitCode code to exit
   */
  protected void exit(int exitCode) {
    ExitUtil.terminate(exitCode);
  }

  /**
   * Get the service name via {@link Service#getName()}.
   * If the service is not instantiated, the classname is returned instead.
   * @return the service name
   */
  public String getServiceName() {
    Service s = service;
    if (s != null) {
      return "service " + s.getName();
    } else {
      return "service classname " + serviceClassName;
    }
  }

  /**
   * Parse the command line, building a configuration from it, then
   * launch the service and wait for it to finish. finally, exit
   * passing the status code to the {@link #exit(int)} method.
   * @param args arguments to the service. arg[0] is 
 * assumed to be the service classname and is automatically
   */
  public void launchServiceAndExit(String[] args) {

    //Currently the config just the default
    Configuration conf = new Configuration();
    String[] processedArgs = extractConfigurationArgs(conf, args);
    int exitCode = launchServiceRobustly(conf, processedArgs);
    exit(exitCode);
  }

  /**
   * Extract the configuration arguments and apply them to the configuration,
   * building an array of processed arguments to hand down to the service.
   * @param conf configuration to update
   * @param args main arguments. args[0] is assumed to be the service
   * classname and is skipped
   * @return the processed list.
   */
  public static String[] extractConfigurationArgs(Configuration conf,
                                              String[] args) {
    //convert args to a list
    List<String> argsList = new ArrayList<String>(args.length - 1);
    for (int index = 1; index < args.length; index++) {
      String arg = args[index];
      if (arg.equals(ARG_CONF)) {
        //the argument is a --conf file tuple: extract the path and load
        //it in as a configuration resource.

        //increment the loop counter
        index++;
        if (index == args.length) {
          //overshot the end of the file
          exitWithMessage(EXIT_COMMAND_ARGUMENT_ERROR,
                          ARG_CONF + ": missing configuration file after ");
        }
        File file = new File(args[index]);
        if (!file.exists()) {
          exitWithMessage(EXIT_COMMAND_ARGUMENT_ERROR,
                          ARG_CONF + ": configuration file not found: "
                          + file);
        }
        try {
          conf.addResource(file.toURI().toURL());
        } catch (MalformedURLException e) {
          exitWithMessage(EXIT_COMMAND_ARGUMENT_ERROR,
                          ARG_CONF + ": configuration file path invalid: "
                          + file);
        }
      } else {
        argsList.add(arg);
      }
    }
    String[] processedArgs = new String[argsList.size()];
    argsList.toArray(processedArgs);
    return processedArgs;
  }

  /**
   * Launch a service catching all excpetions and downgrading them to exit codes
   * after logging.
   * @param conf configuration to use
   * @param processedArgs command line after the launcher-specific arguments have
   * been stripped out
   * @return an exit code.
   */
  public int launchServiceRobustly(Configuration conf,
                                   String[] processedArgs) {
    int exitCode;
    try {
      exitCode = launchService(conf, processedArgs, true);
      if (service != null) {
        Throwable failure = service.getFailureCause();
        if (failure != null) {
          Service.STATE failureState = service.getFailureState();
          if (failureState == Service.STATE.STOPPED) {
            //the failure occurred during shutdown, not important enough to bother
            //the user as it may just scare them
            LOG.debug("Failure during shutdown", failure);
          } else {
            throw failure;
          }
        }
      }
      //either the service succeeded, or an error was only raised during shutdown, 
      //which we don't worry that much about
    } catch (ExitUtil.ExitException exitException) {
      exitCode = exitException.status;
    } catch (Throwable thrown) {
      LOG.error("While running " + getServiceName() + ":" + thrown, thrown);
      if (thrown instanceof GetExceptionExitCode) {
        exitCode = ((GetExceptionExitCode)thrown).getExitCode();
      } else exitCode = EXIT_EXCEPTION_THROWN;
    }
    return exitCode;
  }

  /**
   * Build a log message for starting up and shutting down. 
   * This was grabbed from the ToolRunner code.
   * @param classname the class of the server
   * @param args arguments
   */
  public static String startupShutdownMessage(String classname,
                                            String[] args) {
    final String hostname = NetUtils.getHostname();
    
    return toStartupShutdownString("STARTUP_MSG: ", new String[]{
        "Starting " + classname,
        "  host = " + hostname,
        "  args = " + Arrays.asList(args),
        "  version = " + VersionInfo.getVersion(),
        "  classpath = " + System.getProperty("java.class.path"),
        "  build = " + VersionInfo.getUrl() + " -r "
        + VersionInfo.getRevision()
        + "; compiled by '" + VersionInfo.getUser()
        + "' on " + VersionInfo.getDate(),
        "  java = " + System.getProperty("java.version")
      });
  }

  /**
   * Exit with a printed message
   * @param status status code
   * @param message message
   */
  private static void exitWithMessage(int status, String message) {
    System.err.println(message);
    ExitUtil.terminate(status);
  }
  
  private static String toStartupShutdownString(String prefix, String[] msg) {
    StringBuilder b = new StringBuilder(prefix);
    b.append("\n/************************************************************");
    for (String s : msg) {
      b.append("\n").append(prefix).append(s);
    }
    b.append("\n************************************************************/");
    return b.toString();
  }
  /**
   * forced shutdown runnable.
   */
  protected class ServiceForcedShutdown implements Runnable {

    private final int shutdownTimeMillis;
    private boolean serviceStopped;

    public ServiceForcedShutdown(int shutdownTimeoutMillis) {
      this.shutdownTimeMillis = shutdownTimeoutMillis;
    }
//  @Override

    public void run() {
      if (service != null) {
        service.stop();
        serviceStopped = service.waitForServiceToStop(shutdownTimeMillis);
      } else {
        serviceStopped = true;
      }
    }

    private boolean isServiceStopped() {
      return serviceStopped;
    }
  }

  /**
   * This is the main entry point for the service launcher.
   * @param args command line arguments.
   */
  public static void main(String[] args) {
    if (args.length < 1) {
      exitWithMessage(EXIT_USAGE, USAGE_MESSAGE);
    } else {
      String serviceClassName = args[0];

      if (LOG.isDebugEnabled()) {
        LOG.debug(startupShutdownMessage(serviceClassName, args));
      }
      Thread.setDefaultUncaughtExceptionHandler(
        new YarnUncaughtExceptionHandler());

      ServiceLauncher serviceLauncher = new ServiceLauncher(serviceClassName);
      serviceLauncher.launchServiceAndExit(args);
    }
  }


}
