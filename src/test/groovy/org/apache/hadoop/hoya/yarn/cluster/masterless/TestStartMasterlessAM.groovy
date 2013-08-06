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

package org.apache.hadoop.hoya.yarn.cluster.masterless

import groovy.transform.CompileStatic
import groovy.util.logging.Commons
import org.apache.hadoop.fs.FileSystem as HadoopFS
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hoya.HoyaExitCodes
import org.apache.hadoop.hoya.exceptions.HoyaException
import org.apache.hadoop.hoya.tools.HoyaUtils
import org.apache.hadoop.hoya.yarn.CommonArgs
import org.apache.hadoop.hoya.yarn.HoyaActions
import org.apache.hadoop.hoya.yarn.client.ClientArgs
import org.apache.hadoop.hoya.yarn.client.HoyaClient
import org.apache.hadoop.hoya.yarn.cluster.YarnMiniClusterTestBase
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Test

/**
 * create masterless AMs and work with them. This is faster than
 * bringing up full clusters
 */
@Commons
@CompileStatic
class TestStartMasterlessAM extends YarnMiniClusterTestBase {

  File getConfDirFile() {
    return new File("target/TestStartMasterlessAMconf")
  }

  @Override
  String getConfDir() {
    return confDirFile.toURI().toString()
  }

  @Test
  public void testStartMasterlessAM() throws Throwable {
    String clustername = "TestStartMasterlessAM"
    YarnConfiguration conf = createConfiguration()
    createMiniCluster(clustername, conf, 1, 1, 1, true, true)
    
    describe "create a masterless AM, stop it, restart it"
    //copy the confdir somewhere
    Path resConfPath = new Path(getResourceConfDirURI())
    Path tempConfPath = new Path(confDir)
    HoyaUtils.copyDirectory(conf, resConfPath, tempConfPath)


    ServiceLauncher launcher = createMasterlessAM(clustername, 0, true, true)
    HoyaClient hoyaClient = (HoyaClient) launcher.service
    clusterActionStop(hoyaClient, clustername)

    //here we do something devious: delete our copy of the configuration
    HadoopFS localFS = HadoopFS.get(tempConfPath.toUri(), conf)
    localFS.delete(tempConfPath,true)
    
    //now start the cluster
    ServiceLauncher launcher2 = startHoyaCluster(clustername, [], true);
    HoyaClient newCluster = launcher.getService() as HoyaClient
    newCluster.getClusterStatus(clustername);
  }

  @Test
  public void testStartUnknownCluster() throws Throwable {
    String clustername = "TestStartMasterlessAM"
    createMiniCluster(clustername, createConfiguration(), 1, true)
    try {
      ServiceLauncher launcher = launchHoyaClientAgainstMiniMR(
          createConfiguration(),
          [
              HoyaActions.ACTION_START,
              "no-cluster-of-this-name",
              ClientArgs.ARG_FILESYSTEM, fsDefaultName,
          ])
      assert launcher.serviceExitCode == HoyaExitCodes.EXIT_UNKNOWN_HOYA_CLUSTER
    } catch (HoyaException e) {
      assertExceptionDetails(e,
                             HoyaExitCodes.EXIT_UNKNOWN_HOYA_CLUSTER,
                             HoyaClient.E_UNKNOWN_CLUSTER)
    }
  }

}
