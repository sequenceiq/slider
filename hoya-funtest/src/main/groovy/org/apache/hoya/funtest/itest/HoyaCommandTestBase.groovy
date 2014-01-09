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

package org.apache.hoya.funtest.itest

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.bigtop.itest.shell.Shell
import org.apache.hadoop.hoya.HoyaExitCodes
import org.apache.hadoop.hoya.yarn.Arguments
import org.apache.hadoop.hoya.yarn.HoyaActions
import org.junit.BeforeClass
import org.apache.hadoop.conf.Configuration

@CompileStatic
@Slf4j
class HoyaCommandTestBase {
  private static String USER = System.getProperty("user.name")
  private static String pwd = ""
  private static Configuration conf
  private static Shell bash = new Shell('/bin/bash -s');
  public static final String HOYA_CONF_DIR = "hoya.conf.dir"
  public static final String HOYA_BIN_DIR = "hoya.bin.dir"


  @BeforeClass
  static void setupClass() {
    bash.exec("pwd")
    pwd = bash.out
    int lastIndex = pwd.length() - 1
    pwd = pwd.substring(1, lastIndex)
    Thread.currentThread().name = "junit"

//    JarContent.unpackJarContainer(HoyaCommandTestBase, '.', null);

  }

  /**
   * Exec any hoya command 
   * @param conf
   * @param commands
   * @return the shell
   */
  Shell hoya(List<String> commands) {
    String confDirCmd = "export HOYA_CONF_DIR=${hoyaConfDirectory.toString()};"
    String hoyaCommands = commands.join(" ")
    List<String> commandLine = [
        confDirCmd,
        hoyaScript.absolutePath + " " + hoyaCommands
        ]
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
  Shell hoya(int exitCode, List<String> commands) {
    Shell shell = hoya(commands)
    assertExitCode(shell, exitCode)
  }
  
  public String getHoyaConfDir() { 
    return System.getProperty(HOYA_CONF_DIR)
  }

  /**
   * get the hoya conf dir
   * @return the absolute file of the configuration dir
   */
  public File getHoyaConfDirectory() {
    assert hoyaConfDir
    return new File(hoyaConfDir).absoluteFile
  }

  /**
   * Get the system property for the hoya bin dir -includes
   * an assertion that it is defined
   * @return
   */
  public String getHoyaBinDir() {
    String binDirProp = System.getProperty(HOYA_BIN_DIR)
    assert binDirProp
    return binDirProp
  }
  
  /**
   * Get the directory defined in the hoya.bin.dir syprop
   * @return the directory as a file
   */
  public File getHoyaBinDirectory() {
    String binDirProp = hoyaBinDir
    File dir = new File(binDirProp).absoluteFile
    return dir
  }

  /**
   * Get a file referring to the hoya script
   * @return
   */
  public File getHoyaScript() {
    return new File(hoyaBinDirectory, "bin/hoya")
  }
  
  public void print(Shell shell) {
    List<String> out = shell.out
    shell.err.each { String it -> log.error(it)}
    shell.out.each { String it -> log.info(it)}
  }
  
  Shell freeze(String name) {
    hoya([
         HoyaActions.ACTION_FREEZE, name
    ])
  }  
  Shell freezeForce(String name) {
    hoya([
         HoyaActions.ACTION_FREEZE, Arguments.ARG_FORCE, name
    ])
  }
  
  Shell destroy(String name) {
    hoya([
         HoyaActions.ACTION_DESTROY, name
    ])
  }
    
  Shell list(String name) {

    List<String> cmd = [
        HoyaActions.ACTION_LIST
    ]
    if (name != null) {
      cmd << name
    }
    
    hoya(cmd)
  }
  
  void ensureClusterDestroyed(String name) {
    freezeForce(name)
    destroy(name)
  }

  public File getHoyaClientXMLFile() {
    File dir = hoyaConfDirectory
    File hoyaClientXMLFile = new File(dir, "hoya-client.xml").absoluteFile
    assert hoyaClientXMLFile.exists()
    return hoyaClientXMLFile
  }

  /**
   * Load the client XML file
   * @return
   */
  public Configuration loadClientXML() {
    Configuration conf = new Configuration(true)
    conf.addResource(hoyaClientXMLFile.toURI().toURL())
    return conf
  }

  /**
   * Assert the exit code is that the cluster is unknown
   * @param shell shell
   */
  public void assertSuccess(Shell shell) {
    assertExitCode(shell, 0)
  }
  /**
   * Assert the exit code is that the cluster is unknown
   * @param shell shell
   */
  public void assertUnknownCluster(Shell shell) {
    assertExitCode(shell, HoyaExitCodes.EXIT_UNKNOWN_HOYA_CLUSTER)
  }
  /**
   * Assert a shell exited with a given error code
   * if not the output is printed and an assertion is raised
   * @param shell shell
   * @param errorCode expected error code
   */
  public void assertExitCode(Shell shell, int errorCode) {
    if (shell.ret != errorCode) {
      print(shell)
    }
    assert shell.ret == errorCode
  }
}
