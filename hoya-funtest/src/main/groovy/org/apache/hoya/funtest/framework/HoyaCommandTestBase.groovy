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

import groovy.util.logging.Slf4j
import org.apache.bigtop.itest.shell.Shell
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hoya.HoyaExitCodes
import org.apache.hadoop.hoya.tools.HoyaUtils
import org.apache.hadoop.hoya.yarn.Arguments
import org.apache.hadoop.hoya.yarn.HoyaActions
import org.apache.hoya.testtools.HoyaTestUtils
import org.junit.BeforeClass
import org.apache.hadoop.fs.FileSystem as HadoopFS
import org.junit.Rule
import org.junit.rules.Timeout;

//@CompileStatic
@Slf4j
class HoyaCommandTestBase extends HoyaTestUtils implements HoyaExitCodes {
  private static String USER = System.getProperty("user.name")
  public static final String BASH = '/bin/bash -s'
  private Shell bash = new Shell(BASH);
  public static final String HOYA_CONF_DIR = System.getProperty(
      HoyaTestProperties.HOYA_CONF_DIR_PROP)
  public static final String HOYA_BIN_DIR = System.getProperty(
      HoyaTestProperties.HOYA_BIN_DIR_PROP)
  public static final File HOYA_BIN_DIRECTORY = new File(HOYA_BIN_DIR).canonicalFile
  public static final File HOYA_SCRIPT = new File(HOYA_BIN_DIRECTORY, "bin/hoya").canonicalFile
  public static final File HOYA_CONF_DIRECTORY = new File(
      HOYA_CONF_DIR).canonicalFile
  public static final File HOYA_CONF_XML = new File(HOYA_CONF_DIRECTORY,
                                                    "hoya-client.xml").canonicalFile

  public static final Configuration HOYA_CONFIG
  public static final int THAW_WAIT_TIME
  public static final int FREEZE_WAIT_TIME

  static {
    HOYA_CONFIG = new Configuration(true)
    HOYA_CONFIG.addResource(HOYA_CONF_XML.toURI().toURL())
    THAW_WAIT_TIME = HOYA_CONFIG.getInt(
        HoyaTestProperties.KEY_HOYA_THAW_WAIT_TIME,
        HoyaTestProperties.DEFAULT_HOYA_THAW_WAIT_TIME)
    FREEZE_WAIT_TIME = HOYA_CONFIG.getInt(
        HoyaTestProperties.KEY_HOYA_FREEZE_WAIT_TIME,
        HoyaTestProperties.DEFAULT_HOYA_FREEZE_WAIT_TIME)
  }




  @Rule
  public final Timeout testTimeout = new Timeout(10 * 60 * 1000);


  @BeforeClass
  public static void setupClass() {
    Configuration conf = loadHoyaConf();
    if (HoyaUtils.maybeInitSecurity(conf)) {
      log.debug("Security enabled")
      HoyaUtils.forceLogin()
      }
    }

  /**
   * Exec any hoya command 
   * @param conf
   * @param commands
   * @return the shell
   */
  public static Shell hoya(List<String> commands) {
    String confDirCmd = "export HOYA_CONF_DIR=${HOYA_CONF_DIRECTORY.toString()};"
    String hoyaCommands = HOYA_SCRIPT.absolutePath + " " + commands.join(" ")
    log.info(hoyaCommands)
    List<String> commandLine = [
        confDirCmd,
        hoyaCommands
    ]
    Shell bash = new Shell(BASH);
    String script = commandLine.join("\n")
    log.debug(script)
    return bash.exec(script);
  }

  /**
   * Execute an operation, state the expected error code
   * @param exitCode exit code
   * @param commands commands
   * @return
   */
  public static Shell hoya(int exitCode, List<String> commands) {
    Shell shell = hoya(commands)
    assertExitCode(shell, exitCode)
    return shell
  }

  /**
   * get the hoya conf dir
   * @return the absolute file of the configuration dir
   */
  public static File getHoyaConfDirectory() {
    assert HOYA_CONF_DIR
    return HOYA_CONF_DIRECTORY
  }

  /**
   * Get the directory defined in the hoya.bin.dir syprop
   * @return the directory as a file
   */
  public static File getHoyaBinDirectory() {
    String binDirProp = HOYA_BIN_DIR
    File dir = new File(binDirProp).canonicalFile
    return dir
  }

  /**
   * Get a file referring to the hoya script
   * @return
   */
  public static File getHoyaScript() {
    return new File(HOYA_BIN_DIRECTORY, "bin/hoya")
  }

  public static File getHoyaClientXMLFile() {
    File hoyaClientXMLFile = HOYA_CONF_XML
    assert hoyaClientXMLFile.exists()
    return hoyaClientXMLFile
  }

  /**
   * Load the client XML file
   * @return
   */
  public static Configuration loadHoyaConf() {
    Configuration conf = new Configuration(true)
    conf.addResource(hoyaClientXMLFile.toURI().toURL())
    return conf
  }
  
  public static HadoopFS getClusterFS() {
    return HadoopFS.get(HOYA_CONFIG)
  }
  
  public static void print(Shell shell) {
    List<String> out = shell.out
    shell.err.each { String it -> log.error(it) }
    shell.out.each { String it -> log.info(it) }
  }

  static Shell destroy(String name) {
    hoya([
        HoyaActions.ACTION_DESTROY, name
    ])
  }
  
  static Shell destroy(int result, String name) {
    hoya(result, [
        HoyaActions.ACTION_DESTROY, name
    ])
  }

  static Shell exists(String name) {
    hoya([
        HoyaActions.ACTION_EXISTS, name
    ])
  }

  static Shell exists(int result, String name) {
    hoya(result, [
        HoyaActions.ACTION_EXISTS, name
    ])
  }

  static Shell freeze(String name) {
    hoya([
        HoyaActions.ACTION_FREEZE, name
    ])
  }

  static Shell getConf(String name) {
    hoya([
        HoyaActions.ACTION_GETCONF, name
    ])
  }

  static Shell getConf(int result, String name) {
    hoya(result,
      [
        HoyaActions.ACTION_GETCONF, name
      ])
  }

  static Shell freezeForce(String name) {
    hoya([
        HoyaActions.ACTION_FREEZE, Arguments.ARG_FORCE, name
    ])
  }

  static Shell list(String name) {
    List<String> cmd = [
        HoyaActions.ACTION_LIST
    ]
    if (name != null) {
      cmd << name
    }
    hoya(cmd)
  }

  static Shell list(int result, String name) {
    List<String> cmd = [
        HoyaActions.ACTION_LIST
    ]
    if (name != null) {
      cmd << name
    }
    hoya(result, cmd)
  }

  static Shell status(String name) {
    hoya([
        HoyaActions.ACTION_STATUS, name
    ])
  }
  
  static Shell status(int result, String name) {
    hoya(result,
    [
        HoyaActions.ACTION_STATUS, name
    ])
  }

  static Shell thaw(String name) {
    hoya([
        HoyaActions.ACTION_THAW, name
    ])
  }
  static Shell thaw(int result, String name) {
    hoya(result, 
         [
        HoyaActions.ACTION_THAW, name
    ])
  }

  /**
   * Ensure that a cluster has been destroyed
   * @param name
   */
  static void ensureClusterDestroyed(String name) {
    if (freezeForce(name).ret != EXIT_UNKNOWN_HOYA_CLUSTER) {
      //cluster exists
      destroy(name)
    }
  }

  /**
   * Assert the exit code is that the cluster is unknown
   * @param shell shell
   */
  public static void assertSuccess(Shell shell) {
    assertExitCode(shell, 0)
  }
  /**
   * Assert the exit code is that the cluster is unknown
   * @param shell shell
   */
  public static void assertUnknownCluster(Shell shell) {
    assertExitCode(shell, HoyaExitCodes.EXIT_UNKNOWN_HOYA_CLUSTER)
  }
  
  /**
   * Assert a shell exited with a given error code
   * if not the output is printed and an assertion is raised
   * @param shell shell
   * @param errorCode expected error code
   */
  public static void assertExitCode(Shell shell, int errorCode) {
    assert shell != null
    if (shell.ret != errorCode) {
      print(shell)
    }
    assert shell.ret == errorCode
  }
}
