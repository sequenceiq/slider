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

package org.apache.hoya.yarn.cluster.freezethaw

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.fs.FileSystem as HadoopFS
import org.apache.hadoop.fs.Path
import org.apache.hoya.tools.HoyaUtils
import org.apache.hoya.yarn.client.HoyaClient
import org.apache.hoya.yarn.providers.hbase.HBaseMiniClusterTestBase
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Test

/**
 * create masterless AMs and work with them. This is faster than
 * bringing up full clusters
 */
@CompileStatic
@Slf4j

class TestFreezeThawMasterlessAM extends HBaseMiniClusterTestBase {

  File getConfDirFile() {
    return new File("target/TestFreezeThawMasterlessAM/conf")
  }

  @Override
  String getConfDir() {
    return confDirFile.toURI().toString()
  }

  @Test
  public void testFreezeThawMasterlessAM() throws Throwable {
    String clustername = "test_freeze_thaw_masterless_am"
    YarnConfiguration conf = getConfiguration()
    createMiniCluster(clustername, conf, 1, 1, 1, true, true)
    
    describe "create a masterless AM, freeze it, thaw it"
    //copy the confdir somewhere
    Path resConfPath = new Path(getResourceConfDirURI())
    Path tempConfPath = new Path(confDir)
    HoyaUtils.copyDirectory(conf, resConfPath, tempConfPath, null)


    ServiceLauncher launcher = createMasterlessAM(clustername, 0, true, true)
    HoyaClient hoyaClient = (HoyaClient) launcher.service
    addToTeardown(hoyaClient);

    clusterActionFreeze(hoyaClient, clustername)

    // here we do something devious: delete our copy of the configuration
    // this makes sure the remote config gets picked up
    HadoopFS localFS = HadoopFS.get(tempConfPath.toUri(), conf)
    localFS.delete(tempConfPath,true)
    
    //now start the cluster
    ServiceLauncher launcher2 = thawHoyaCluster(clustername, [], true);
    HoyaClient newCluster = launcher.getService() as HoyaClient
    newCluster.getClusterDescription(clustername);
  }

}
