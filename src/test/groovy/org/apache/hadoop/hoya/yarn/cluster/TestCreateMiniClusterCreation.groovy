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

package org.apache.hadoop.hoya.yarn.cluster

import groovy.util.logging.Slf4j
import org.apache.hadoop.hoya.providers.hbase.HBaseKeys
import org.apache.hadoop.hoya.yarn.providers.hbase.HBaseMiniClusterTestBase
import org.junit.Test

/**
 * Test of RM creation. This is so the later test's prereq's can be met
 */
//@CompileStatic
@Slf4j

class TestCreateMiniClusterCreation extends HBaseMiniClusterTestBase {

  @Test
  public void testHoyaTestConfigFound() throws Throwable {
    describe("verify that hbase home points to a cluster configuration dir")
    String hbaseHome = getHBaseHome()

    assert hbaseHome != null && !hbaseHome.isEmpty()
    File hbaseHomeDir = new File(hbaseHome)
    assert hbaseHomeDir.exists()
    assert hbaseHomeDir.isDirectory()
    File hbaseBinDir = new File(hbaseHomeDir, "bin")
    assert hbaseBinDir.exists() && hbaseHomeDir.isDirectory()
    File hbaseShell = new File(hbaseBinDir, "hbase")
    assert hbaseShell.exists()
    File hbaseSite = new File(hbaseHomeDir, "conf/" + HBaseKeys.HBASE_SITE);
    assert hbaseSite.exists()

  }


  @Test
  public void testYARNClusterCreation() throws Throwable {
    describe "Create a mini cluster"
    createMiniCluster("testYARNClusterCreation", createConfiguration(), 1, true)
    String rmAddr = getRMAddr();

    log.info("RM address = $rmAddr")
  }
}
