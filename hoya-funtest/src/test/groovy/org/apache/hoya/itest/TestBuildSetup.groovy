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

package org.apache.hoya.itest

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hoya.funtest.itest.HoyaCommandTestBase
import org.apache.hoya.funtest.itest.HoyaTestProperties
import org.junit.Test

import org.apache.hadoop.fs.FileSystem as HadoopFS;

/**
 * Simple tests to verify that the build has been set up: if these
 * fail then the arguments to the test run are incomplete.
 */
@CompileStatic
@Slf4j
class TestBuildSetup extends HoyaCommandTestBase implements HoyaTestProperties {


  @Test
  public void testConfDirSet() throws Throwable {
    assert HOYA_CONF_DIR
  }


  @Test
  public void testConfDirExists() throws Throwable {
    assert hoyaConfDirectory.exists()
  }


  @Test
  public void testConfDirHasHoyaClientXML() throws Throwable {
    File hoyaClientXMLFile = hoyaClientXMLFile
    hoyaClientXMLFile.toString()
  }


  @Test
  public void testBinDirExists() throws Throwable {
    String binDirProp = HOYA_BIN_DIR
    assert binDirProp
    File dir = new File(binDirProp).absoluteFile
    assert dir.exists()
  }


  @Test
  public void testBinScriptExists() throws Throwable {
    assert hoyaScript.exists()
  }

  @Test
  public void testConfLoad() throws Throwable {
    Configuration conf = loadHoyaConf()
    String fs = conf.get("fs.defaultFS")
    assert fs != null
  }


  @Test
  public void testConfHasRM() throws Throwable {
    Configuration conf = loadHoyaConf()
    String val = conf.get(YarnConfiguration.RM_ADDRESS)
    log.debug("$YarnConfiguration.DEFAULT_RM_ADDRESS = $val")
    assert val != YarnConfiguration.DEFAULT_RM_ADDRESS
  }


  @Test
  public void testImageExists() throws Throwable {


    Configuration conf = loadHoyaConf()
    String testImage = conf.get(KEY_HOYA_TEST_HBASE_TAR)
    assert testImage
    Path path = new Path(testImage)
    HadoopFS fs = HadoopFS.get(path.toUri(), conf)
    assert fs.exists(path)
  }

  @Test
  public void testAppConfExists() throws Throwable {


    Configuration conf = loadHoyaConf()
    String dir = conf.get(KEY_HOYA_TEST_HBASE_APPCONF)
    assert dir
    Path path = new Path(dir)
    HadoopFS fs = HadoopFS.get(path.toUri(), conf)
    assert fs.exists(path)
  }

}
