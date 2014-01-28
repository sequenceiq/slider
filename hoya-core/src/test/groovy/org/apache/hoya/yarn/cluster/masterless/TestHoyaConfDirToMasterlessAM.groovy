/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *   
 *    http://www.apache.org/licenses/LICENSE-2.0
 *   
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License. See accompanying LICENSE file.
 */

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

package org.apache.hoya.yarn.cluster.masterless

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.fs.Path
import org.apache.hoya.HoyaKeys
import org.apache.hoya.api.ClusterDescription
import org.apache.hoya.tools.HoyaFileSystem
import org.apache.hoya.tools.HoyaUtils
import org.apache.hoya.yarn.client.HoyaClient
import org.apache.hoya.yarn.providers.hbase.HBaseMiniClusterTestBase
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.apache.hadoop.fs.FileSystem as HadoopFS

import org.junit.Test

/**
 * create masterless AMs and work with them. This is faster than
 * bringing up full clusters
 */
@CompileStatic
@Slf4j

class TestHoyaConfDirToMasterlessAM extends HBaseMiniClusterTestBase {


  @Test
  public void testHoyaConfDirToMasterlessAM() throws Throwable {
    String clustername = "test_hoya_conf_dir_to_masterless_am"
    YarnConfiguration conf = getConfiguration()
    createMiniCluster(clustername, conf, 1, true)

    describe "kill a masterless AM and verify that it shuts down"

    File localConf = File.createTempDir("conf","dir")
    String name = "hoya.xml"
    File hoyaXML = new File(localConf, name)
    def out = new FileWriter(hoyaXML)
    out.write(['a','b','c'] as char[])
    out.write("as string")
    out.flush()
    out.close()
    try {
      System.setProperty(HoyaKeys.PROPERTY_HOYA_CONF_DIR,localConf.absolutePath);
      ServiceLauncher launcher = createMasterlessAM(clustername, 0, true, true)
      HoyaClient hoyaClient = (HoyaClient) launcher.service
      addToTeardown(hoyaClient);
      ApplicationReport report = waitForClusterLive(hoyaClient)

      ClusterDescription cd = waitForRoleCount(hoyaClient,HoyaKeys.ROLE_HOYA_AM,1, HBASE_CLUSTER_STARTUP_TIME)
      HadoopFS fs = HadoopFS.getLocal(conf);
      
      Path clusterDir = new HoyaFileSystem(fs).buildHoyaClusterDirPath(clustername)
      assert fs.exists(clusterDir);
      Path hoyaConfDir = new Path(clusterDir, HoyaKeys.SUBMITTED_HOYA_CONF_DIR)
      assert fs.exists(hoyaConfDir);
      Path remoteXml = new Path(hoyaConfDir,name)
      assert fs.exists(remoteXml)
      

      clusterActionFreeze(hoyaClient, clustername)
    } finally {
      HoyaUtils.deleteDirectoryTree(localConf)
    }



  }


}
