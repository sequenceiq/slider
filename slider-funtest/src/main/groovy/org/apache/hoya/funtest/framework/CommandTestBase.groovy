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

package org.apache.hoya.funtest.framework

import groovy.transform.CompileStatic
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem as HadoopFS
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.ExitUtil
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.apache.hoya.api.ClusterDescription
import org.apache.hoya.exceptions.HoyaException
import org.apache.hoya.testtools.HoyaTestUtils
import org.apache.hoya.tools.HoyaUtils
import org.apache.hoya.yarn.Arguments
import org.apache.hoya.yarn.client.HoyaClient
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.rules.Timeout
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import static org.apache.hoya.HoyaExitCodes.*
import static FuntestProperties.*
import static org.apache.hoya.yarn.Arguments.*
import static org.apache.hoya.yarn.HoyaActions.*
import static org.apache.hoya.HoyaXMLConfKeysForTesting.*

@CompileStatic
abstract class CommandTestBase extends HoyaTestUtils {
  private static final Logger log =
      LoggerFactory.getLogger(CommandTestBase.class);
  
  public static final String BASH = '/bin/bash -s'
  public static final String HOYA_CONF_DIR = System.getProperty(
      HOYA_CONF_DIR_PROP)
  public static final String HOYA_BIN_DIR = System.getProperty(
      HOYA_BIN_DIR_PROP)
  public static final File HOYA_BIN_DIRECTORY = new File(
      HOYA_BIN_DIR).canonicalFile
  public static final File HOYA_SCRIPT = new File(
      HOYA_BIN_DIRECTORY,
      "bin/slider").canonicalFile
  public static final File HOYA_CONF_DIRECTORY = new File(
      HOYA_CONF_DIR).canonicalFile
  public static final File HOYA_CONF_XML = new File(HOYA_CONF_DIRECTORY,
      CLIENT_CONFIG_FILENAME).canonicalFile

  public static final YarnConfiguration HOYA_CONFIG
  public static final int THAW_WAIT_TIME
  public static final int FREEZE_WAIT_TIME
  public static final int HBASE_LAUNCH_WAIT_TIME
  public static final int ACCUMULO_LAUNCH_WAIT_TIME
  public static final int HOYA_TEST_TIMEOUT
  public static final boolean ACCUMULO_TESTS_ENABLED
  public static final boolean HBASE_TESTS_ENABLED
  public static final boolean FUNTESTS_ENABLED
  public static final boolean AGENTESTS_ENABLED


  static {
    HOYA_CONFIG = new YarnConfiguration()
    HOYA_CONFIG.addResource(HOYA_CONF_XML.toURI().toURL())
    THAW_WAIT_TIME = HOYA_CONFIG.getInt(
        KEY_HOYA_THAW_WAIT_TIME,
        DEFAULT_HOYA_THAW_WAIT_TIME)
    FREEZE_WAIT_TIME = HOYA_CONFIG.getInt(
        KEY_HOYA_FREEZE_WAIT_TIME,
        DEFAULT_HOYA_FREEZE_WAIT_TIME)
    HBASE_LAUNCH_WAIT_TIME = HOYA_CONFIG.getInt(
        KEY_HOYA_HBASE_LAUNCH_TIME,
        DEFAULT_HOYA_HBASE_LAUNCH_TIME)
    HOYA_TEST_TIMEOUT = HOYA_CONFIG.getInt(
        KEY_HOYA_TEST_TIMEOUT,
        DEFAULT_HOYA_TEST_TIMEOUT)
    ACCUMULO_LAUNCH_WAIT_TIME = HOYA_CONFIG.getInt(
        KEY_HOYA_ACCUMULO_LAUNCH_TIME,
        DEFAULT_HOYA_ACCUMULO_LAUNCH_TIME)
    FUNTESTS_ENABLED =
        HOYA_CONFIG.getBoolean(KEY_HOYA_FUNTESTS_ENABLED, true)
    ACCUMULO_TESTS_ENABLED =
        HOYA_CONFIG.getBoolean(KEY_HOYA_TEST_ACCUMULO_ENABLED, false)
    HBASE_TESTS_ENABLED =
        HOYA_CONFIG.getBoolean(KEY_HOYA_TEST_HBASE_ENABLED, true)
 }

  @Rule
  public final Timeout testTimeout = new Timeout(HOYA_TEST_TIMEOUT);


  @BeforeClass
  public static void setupClass() {
    Configuration conf = loadHoyaConf();
    if (HoyaUtils.maybeInitSecurity(conf)) {
      log.debug("Security enabled")
      HoyaUtils.forceLogin()
    } else {
      log.info "Security off, making cluster dirs broadly accessible"
    }
    SliderShell.confDir = HOYA_CONF_DIRECTORY
    SliderShell.script = HOYA_SCRIPT
    log.info("Test using ${HadoopFS.getDefaultUri(HOYA_CONFIG)} " +
             "and YARN RM @ ${HOYA_CONFIG.get(YarnConfiguration.RM_ADDRESS)}")
  }

  /**
   * Exec any hoya command 
   * @param conf
   * @param commands
   * @return the shell
   */
  public static SliderShell hoya(List<String> commands) {
    SliderShell shell = new SliderShell(commands)
    shell.execute()
    return shell
  }

  /**
   * Execute an operation, state the expected error code
   * @param exitCode exit code
   * @param commands commands
   * @return
   */
  public static SliderShell hoya(int exitCode, List<String> commands) {
    return SliderShell.run(commands, exitCode)
  }

  /**
   * Load the client XML file
   * @return
   */
  public static Configuration loadHoyaConf() {
    Configuration conf = new Configuration(true)
    conf.addResource(HOYA_CONF_XML.toURI().toURL())
    return conf
  }

  public static HadoopFS getClusterFS() {
    return HadoopFS.get(HOYA_CONFIG)
  }


  static SliderShell destroy(String name) {
    hoya([
        ACTION_DESTROY, name
    ])
  }

  static SliderShell destroy(int result, String name) {
    hoya(result, [
        ACTION_DESTROY, name
    ])
  }

  static SliderShell exists(String name, boolean live = true) {

    List<String> args = [
        ACTION_EXISTS, name
    ]
    if (live) {
      args << Arguments.ARG_LIVE
    }
    hoya(args)
  }

  static SliderShell exists(int result, String name, boolean live = true) {
    List<String> args = [
        ACTION_EXISTS, name
    ]
    if (live) {
      args << ARG_LIVE
    }
    hoya(result, args)
  }

  static SliderShell freeze(String name) {
    hoya([
        ACTION_FREEZE, name
    ])
  }

  static SliderShell getConf(String name) {
    hoya([
        ACTION_GETCONF, name
    ])
  }

  static SliderShell getConf(int result, String name) {
    hoya(result,
         [
             ACTION_GETCONF, name
         ])
  }

  static SliderShell killContainer(String name, String containerID) {
    hoya(0,
         [
             ACTION_KILL_CONTAINER,
             name,
             containerID
         ])
  }
  
  static SliderShell freezeForce(String name) {
    hoya([
        ACTION_FREEZE, ARG_FORCE, name
    ])
  }

  static SliderShell list(String name) {
    List<String> cmd = [
        ACTION_LIST
    ]
    if (name != null) {
      cmd << name
    }
    hoya(cmd)
  }

  static SliderShell list(int result, String name) {
    List<String> cmd = [
        ACTION_LIST
    ]
    if (name != null) {
      cmd << name
    }
    hoya(result, cmd)
  }

  static SliderShell status(String name) {
    hoya([
        ACTION_STATUS, name
    ])
  }

  static SliderShell status(int result, String name) {
    hoya(result,
         [
             ACTION_STATUS, name
         ])
  }

  static SliderShell thaw(String name) {
    hoya([
        ACTION_THAW, name
    ])
  }

  static SliderShell thaw(int result, String name) {
    hoya(result,
         [
             ACTION_THAW, name
         ])
  }

  /**
   * Ensure that a cluster has been destroyed
   * @param name
   */
  static void ensureClusterDestroyed(String name) {
    def froze = freezeForce(name)

    def result = froze.ret
    if (result != 0 && result != EXIT_UNKNOWN_HOYA_CLUSTER) {
      froze.assertExitCode(0)
    }
    destroy(0, name)
  }

  /**
   * If the functional tests are enabled, set up the cluster
   * 
   * @param cluster
   */
  static void setupCluster(String cluster) {
    if (FUNTESTS_ENABLED) {
      ensureClusterDestroyed(cluster)
    }
  }

  /**
   * Teardown operation -freezes cluster, and may destroy it
   * though for testing it is best if it is retained
   * @param name cluster name
   */
  static void teardown(String name) {
    if (FUNTESTS_ENABLED) {
      freezeForce(name)
    }
  }

  /**
   * Assert the exit code is that the cluster is unknown
   * @param shell shell
   */
  public static void assertSuccess(SliderShell shell) {
    assertExitCode(shell, 0)
  }
  /**
   * Assert the exit code is that the cluster is unknown
   * @param shell shell
   */
  public static void assertUnknownCluster(SliderShell shell) {
    assertExitCode(shell, EXIT_UNKNOWN_HOYA_CLUSTER)
  }

  /**
   * Assert a shell exited with a given error code
   * if not the output is printed and an assertion is raised
   * @param shell shell
   * @param errorCode expected error code
   */
  public static void assertExitCode(SliderShell shell, int errorCode) {
    shell.assertExitCode(errorCode)
  }

  /**
   * Create a connection to the cluster by execing the status command
   *
   * @param clustername
   * @return
   */
  HoyaClient bondToCluster(Configuration conf, String clustername) {

    String address = getRequiredConfOption(conf, YarnConfiguration.RM_ADDRESS)

    ServiceLauncher<HoyaClient> launcher = launchHoyaClientAgainstRM(
        address,
        ["exists", clustername],
        conf)

    int exitCode = launcher.serviceExitCode
    if (exitCode) {
      throw new ExitUtil.ExitException(exitCode, "exit code = $exitCode")
    }
    HoyaClient hoyaClient = launcher.service
    hoyaClient.deployedClusterName = clustername
    return hoyaClient;
  }

  /**
   * Create or build a hoya cluster (the action is set by the first verb)
   * @param action operation to invoke: ACTION_CREATE or ACTION_BUILD
   * @param clustername cluster name
   * @param roles map of rolename to count
   * @param extraArgs list of extra args to add to the creation command
   * @param deleteExistingData should the data of any existing cluster
   * of this name be deleted
   * @param blockUntilRunning block until the AM is running
   * @param clusterOps map of key=value cluster options to set with the --option arg
   * @return shell which will have executed the command.
   */
  public SliderShell createOrBuildHoyaCluster(
      String action,
      String clustername,
      Map<String, Integer> roles,
      List<String> extraArgs,
      boolean blockUntilRunning,
      Map<String, String> clusterOps) {
    assert action != null
    assert clustername != null



    List<String> roleList = [];
    roles.each { String role, Integer val ->
      log.info("Role $role := $val")
      roleList << ARG_COMPONENT << role << Integer.toString(val)
    }

    List<String> argsList = [action, clustername]

    argsList << ARG_ZKHOSTS <<
    HOYA_CONFIG.getTrimmed(KEY_HOYA_TEST_ZK_HOSTS, DEFAULT_HOYA_ZK_HOSTS)


    if (blockUntilRunning) {
      argsList << ARG_WAIT << Integer.toString(THAW_WAIT_TIME)
    }

    argsList += roleList;

    //now inject any cluster options
    clusterOps.each { String opt, String val ->
      argsList << ARG_OPTION << opt.toString() << val.toString();
    }

    if (extraArgs != null) {
      argsList += extraArgs;
    }
    hoya(0, argsList)
  }

  /**
   * Create a hoya cluster
   * @param clustername cluster name
   * @param roles map of rolename to count
   * @param extraArgs list of extra args to add to the creation command
   * @param blockUntilRunning block until the AM is running
   * @param clusterOps map of key=value cluster options to set with the --option arg
   * @return launcher which will have executed the command.
   */
  public SliderShell createHoyaCluster(
      String clustername,
      Map<String, Integer> roles,
      List<String> extraArgs,
      boolean blockUntilRunning,
      Map<String, String> clusterOps) {
    return createOrBuildHoyaCluster(
        ACTION_CREATE,
        clustername,
        roles,
        extraArgs,
        blockUntilRunning,
        clusterOps)
  }

  public Path buildClusterPath(String clustername) {
    return new Path(clusterFS.homeDirectory, ".hoya/cluster/${clustername}")
  }


  public ClusterDescription killAmAndWaitForRestart(
      HoyaClient hoyaClient, String cluster) {
    
    assert cluster
    hoya(0, [
        ACTION_AM_SUICIDE, cluster,
        ARG_EXITCODE, "1",
        ARG_WAIT, "1000",
        ARG_MESSAGE, "suicide"
    ])



    def sleeptime = HOYA_CONFIG.getInt( KEY_AM_RESTART_SLEEP_TIME,
                                        DEFAULT_AM_RESTART_SLEEP_TIME)
    sleep(sleeptime)
    ClusterDescription status

    try {
      // am should have restarted it by now
      // cluster is live
      exists(0, cluster, true)

      status = hoyaClient.clusterDescription
    } catch (HoyaException e) {
      if (e.exitCode == EXIT_BAD_CLUSTER_STATE) {
        log.error(
            "Property $YarnConfiguration.RM_AM_MAX_ATTEMPTS may be too low")
      }
      throw e;
    }
    return status
  }

  /**
   * if tests are not enabled: skip them  
   */
  public static void assumeFunctionalTestsEnabled() {
    assume(FUNTESTS_ENABLED, "Functional tests disabled")
  }

  public static void assumeAccumuloTestsEnabled() {
    assumeFunctionalTestsEnabled()
    assume(ACCUMULO_TESTS_ENABLED, "Accumulo tests disabled")
  }

  public void assumeHBaseTestsEnabled() {
    assumeFunctionalTestsEnabled()
    assume(HBASE_TESTS_ENABLED, "HBase tests disabled")
  }
  
  public void assumeAgentTestsEnabled() {
    assumeFunctionalTestsEnabled()
    assume(HBASE_TESTS_ENABLED, "HBase tests disabled")
  }

}
