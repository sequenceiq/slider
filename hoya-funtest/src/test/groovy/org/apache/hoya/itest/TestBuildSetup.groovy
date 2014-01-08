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

import org.apache.hoya.funtest.itest.HoyaCommandTestBase
import org.junit.Test

class TestBuildSetup extends HoyaCommandTestBase {


  @Test
  public void testConfDirSet() throws Throwable {
    String hoyaConfDir = System.getProperty(HOYA_CONF_DIR)
    assert hoyaConfDir
  }
  
  @Test
  public void testConfDirExists() throws Throwable {
    String hoyaConfDir = System.getProperty(HOYA_CONF_DIR)
    File dir = new File(hoyaConfDir).absoluteFile
    assert dir.exists()
  }
  
  @Test
  public void testConfDirHasHoyaClientXML() throws Throwable {
    String hoyaConfDir = System.getProperty(HOYA_CONF_DIR)
    File dir = new File(hoyaConfDir)
    File hoyaClientXMLFile = new File(dir, "hoya-client.xml").absoluteFile
    
    assert hoyaClientXMLFile.exists()
  }


  @Test
  public void testBinDirExists() throws Throwable {
    String binDirProp = System.getProperty(HOYA_BIN_DIR)
    assert binDirProp
    File dir = new File(binDirProp).absoluteFile
    assert dir.exists()
  }
  
  @Test
  public void testBinScriptExists() throws Throwable {
    String binDirProp = System.getProperty(HOYA_BIN_DIR)
    assert binDirProp
    File dir = new File(binDirProp)
    File binFile = new File(dir, "bin/hoya").absoluteFile

    assert binFile.exists()
  }
  
  
}
