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

package org.apache.hoya.funtest.basic

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hoya.funtest.framework.HoyaFuntestProperties
import org.apache.hoya.testtools.HoyaTestUtils
import org.apache.hoya.tools.HoyaUtils
import org.junit.Test

/**
 * Simple tests to verify that the build has been set up: if these
 * fail then the arguments to the test run are incomplete.
 * 
 * This deliberately doesn't depend on HoyaCommandTestBase,
 * so that individual tests fail with more diagnostics
 * than the @BeforeClass failing
 */
@CompileStatic
@Slf4j
class TestBuildSetup extends HoyaTestUtils implements HoyaFuntestProperties {

/*
  String HOYA_BIN_DIR = System.getProperty(
      HoyaTestProperties.HOYA_BIN_DIR_PROP)
  File HOYA_BIN_DIRECTORY = new File(
      HOYA_BIN_DIR).canonicalFile
  public static final File HOYA_SCRIPT = new File(
      HOYA_BIN_DIRECTORY,
      "bin/hoya").canonicalFile
  public static final File HOYA_CONF_DIRECTORY = new File(
      HOYA_CONF_DIR).canonicalFile
  public static final File HOYA_CONF_XML = new File(HOYA_CONF_DIRECTORY,
                                                    "hoya-client.xml").canonicalFile
*/


  @Test
  public void testConfDirSet() throws Throwable {
    assert getHoyaConfDirProp()
    log.info("Hoya Configuration directory $hoyaConfDirProp")
  }

  String getRequiredSysprop(String name) {
    String val = System.getProperty(name)
    if (!val) {
      fail("System property not set : $name")
      
    }
    return val;
  }

  public String getHoyaConfDirProp() {
    return getRequiredSysprop(HOYA_CONF_DIR_PROP)
  }

  public File getHoyaConfDirectory() {
    return getCanonicalFile(hoyaConfDirProp)
  }

  File getCanonicalFile(String dir) {
    assert dir
    return new File(dir).canonicalFile
  }

  File getHoyaBinDirectory() {
    return getCanonicalFile(getHoyaBinDirProp())
  }

  public String getHoyaBinDirProp() {
    return getRequiredSysprop(HOYA_BIN_DIR_PROP)
  }

  public File getHoyaConfXML() {
    new File(hoyaConfDirectory,
                             "hoya-client.xml").canonicalFile
  }
  
  public File getHoyaScript() {
    new File(hoyaBinDirectory, "bin/hoya").canonicalFile
  }

  /**
   * Load the client XML file
   * @return
   */
  public Configuration loadHoyaConf() {
    Configuration conf = new Configuration(true)
    conf.addResource(hoyaConfXML.toURI().toURL())
    return conf
  }

  @Test
  public void testConfDirExists() throws Throwable {
    assert hoyaConfDirectory.exists()
  }

  

  @Test
  public void testConfDirHasHoyaClientXML() throws Throwable {
    File hoyaClientXMLFile = hoyaConfXML
    assert hoyaClientXMLFile.exists()
    hoyaClientXMLFile.toString()}


  @Test
  public void testBinDirExists() throws Throwable {
    log.info("Hoya binaries dir = $hoyaBinDirectory")
  }


  @Test
  public void testBinScriptExists() throws Throwable {
    assert hoyaScript.exists()
  }

  @Test
  public void testConfLoad() throws Throwable {
    Configuration conf = loadHoyaConf()
    String fs = conf.get("fs.defaultFS")
    log.info("Test Filesystem $fs")
    assert fs != null
  }


  @Test
  public void testConfHasRM() throws Throwable {
    Configuration conf = loadHoyaConf()
    String val = conf.get(YarnConfiguration.RM_ADDRESS)
    log.info("$YarnConfiguration.RM_ADDRESS = $val")
    assert val != YarnConfiguration.DEFAULT_RM_ADDRESS
  }

@Test
  public void testSecuritySettingsValid() throws Throwable {
    Configuration conf = loadHoyaConf();
    if (HoyaUtils.maybeInitSecurity(conf)) {
      log.info("Security enabled")
      HoyaUtils.forceLogin()
    }
    log.info("Login User = ${UserGroupInformation.getLoginUser()}")
  }
}
