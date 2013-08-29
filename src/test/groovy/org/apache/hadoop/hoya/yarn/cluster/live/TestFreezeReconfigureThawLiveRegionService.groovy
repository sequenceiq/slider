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

package org.apache.hadoop.hoya.yarn.cluster.live

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem as HadoopFS
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.ClusterStatus
import org.apache.hadoop.hoya.api.ClusterDescription
import org.apache.hadoop.hoya.providers.hbase.HBaseKeys
import org.apache.hadoop.hoya.tools.ConfigHelper
import org.apache.hadoop.hoya.tools.HoyaUtils
import org.apache.hadoop.hoya.yarn.client.HoyaClient
import org.apache.hadoop.hoya.yarn.cluster.YarnMiniClusterTestBase
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.junit.Test

/**
 * Test of RM creation. This is so the later test's prereq's can be met
 */
@CompileStatic
@Slf4j
class TestFreezeReconfigureThawLiveRegionService extends YarnMiniClusterTestBase {

  @Test
  public void testFreezeReconfigureThawLiveRegionService() throws Throwable {
    String clustername = "TestFreezeReconfigureThawLiveRegionService"
    int regionServerCount = 1
    createMiniCluster(clustername, createConfiguration(), 1, true)
    describe("Create a cluster, freeze it, patch the configuration files," +
             " thaw it and verify that it came back with the new settings")

    ServiceLauncher launcher = createHoyaCluster(clustername, regionServerCount, [], true, true)
    HoyaClient hoyaClient = (HoyaClient) launcher.service
    addToTeardown(hoyaClient);
    ClusterDescription status = hoyaClient.getClusterStatus(clustername)
    log.info("${status.toJsonString()}")

    ClusterStatus clustat = basicHBaseClusterStartupSequence(hoyaClient)

    clustat = waitForHBaseRegionServerCount(hoyaClient, clustername, regionServerCount,
                            HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)
    describe("Cluster status")
    log.info(statusToString(clustat));
    

    clusterActionFreeze(hoyaClient, clustername)
    killAllRegionServers();
    
    //reconfigure time
    
    //get the location of the cluster
    HadoopFS dfs = HadoopFS.get(new URI(fsDefaultName), miniCluster.config)
    Path clusterDir = HoyaUtils.buildHoyaClusterDirPath(dfs, clustername);
    Path specPath = HoyaUtils.locateClusterSpecification(dfs, clustername);
    ClusterDescription persistedSpec = HoyaUtils.loadAndValidateClusterSpec(dfs, specPath);
    Path confdir = new Path(persistedSpec.originConfigurationPath);
    Path hbaseSiteXML = new Path(confdir, HBaseKeys.HBASE_SITE)
    Configuration originalConf = ConfigHelper.loadTemplateConfiguration(dfs, hbaseSiteXML, "");
    //patch
    String patchedText = "patched-after-freeze"
    originalConf.setBoolean(patchedText,true);
    //save
    ConfigHelper.saveConfig(dfs, hbaseSiteXML,originalConf);

        //now let's start the cluster up again
    ServiceLauncher launcher2 = thawHoyaCluster(clustername, [], true);
    HoyaClient thawed = launcher2.service as HoyaClient
    clustat = basicHBaseClusterStartupSequence(thawed)

    //get the options
    ClusterDescription stat = thawed.clusterStatus
    Map<String, String> properties = stat.clientProperties
    log.info("Cluster properties: \n"+HoyaUtils.stringifyMap(properties));
    assert properties[patchedText]=="true";
    
  }




}
