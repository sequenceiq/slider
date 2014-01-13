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

package org.apache.hoya.testtools

import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.hadoop.conf.Configuration
import org.apache.hoya.api.ClusterDescription
import org.apache.hoya.api.ClusterNode
import org.apache.hoya.exceptions.HoyaException
import org.apache.hoya.exceptions.WaitTimeoutException
import org.apache.hoya.providers.hbase.HBaseKeys
import org.apache.hoya.tools.Duration
import org.apache.hoya.yarn.client.HoyaClient
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.junit.Assert
import org.junit.Assume

/**
 * Static utils for tests in this package and in other test projects.
 * 
 * It is designed to work with mini clusters as well as remote ones
 * 
 * This class is not final and may be extended for test cases
 */
@Slf4j
@CompileStatic
class HoyaTestUtils extends Assert {

  public static void describe(String s) {
    log.info("");
    log.info("===============================");
    log.info(s);
    log.info("===============================");
    log.info("");
  }

  public static String prettyPrint(String json) {
    JsonOutput.prettyPrint(json)
  }

  public static void skip(String message) {
    Assume.assumeTrue(message, false);
  }


  public static void assertListEquals(List left, List right) {
    assert left.size() == right.size();
    for (int i = 0; i < left.size(); i++) {
      assert left[0] == right[0]
    }
  }


  public static void assumeConfOptionSet(Configuration conf, String key) {
    Assume.assumeNotNull("not defined " + key, conf.get(key))
  }

  /**
   * Wait for the cluster live; fail if it isn't within the (standard) timeout
   * @param hoyaClient client
   * @return the app report of the live cluster
   */
  public static ApplicationReport waitForClusterLive(HoyaClient hoyaClient,int goLiveTime) {
    ApplicationReport report = hoyaClient.monitorAppToRunning(
        new Duration(goLiveTime));
    assertNotNull(
        "Cluster did not go live in the time $goLiveTime",
        report);
    return report;
  }



  public static void waitWhileClusterExists(HoyaClient client, int timeout) {
    Duration duration = new Duration(timeout);
    duration.start()
    while (client.actionExists(client.deployedClusterName) &&
           !duration.limitExceeded) {
      sleep(1000);
    }
  }

  /**
   * Spin waiting for the Hoya role count to match expected
   * @param hoyaClient client
   * @param role role to look for
   * @param desiredCount RS count
   * @param timeout timeout
   */
  public static ClusterDescription waitForRoleCount(
      HoyaClient hoyaClient,
      String role,
      int desiredCount,
      int timeout) {
    return waitForRoleCount(hoyaClient, [(role): desiredCount], timeout)
  }

  /**
   * Spin waiting for the Hoya role count to match expected
   * @param hoyaClient client
   * @param roles map of roles to look for
   * @param desiredCount RS count
   * @param timeout timeout
   */
  public static ClusterDescription waitForRoleCount(
      HoyaClient hoyaClient,
      Map<String, Integer> roles,
      int timeout,
      String operation = "startup") {
    String clustername = hoyaClient.deployedClusterName;
    ClusterDescription status = null
    Duration duration = new Duration(timeout);
    duration.start()
    boolean roleCountFound = false;
    while (!roleCountFound) {
      StringBuilder details = new StringBuilder()
      roleCountFound = true;
      status = hoyaClient.getClusterDescription(clustername)

      for (Map.Entry<String, Integer> entry : roles.entrySet()) {
        String role = entry.key
        int desiredCount = entry.value
        Integer instances = status.instances[role];
        int instanceCount = instances != null ? instances.intValue() : 0;
        if (instanceCount != desiredCount) {
          roleCountFound = false;
        }
        details.append("[$role]: $instanceCount of $desiredCount; ")
      }
      if (roleCountFound) {
        //successful
        log.info("$operation: role count as desired: $details")

        break;
      }

      if (duration.limitExceeded) {
        duration.finish();
        describe("$operation: role count not met after $duration: $details")
        log.info(prettyPrint(status.toJsonString()))
        fail(
            "$operation: role counts not met  after $duration: $details in \n$status ")
      }
      log.info("Waiting: " + details)
      Thread.sleep(1000)
    }
    return status
  }

  /**
   * Wait for the hbase master to be live (or past it in the lifecycle)
   * @param clustername cluster
   * @param spintime time to wait
   * @return true if the cluster came out of the sleep time live 
   * @throws IOException
   * @throws org.apache.hoya.exceptions.HoyaException
   */
  public static boolean spinForClusterStartup(HoyaClient hoyaClient, long spintime)
      throws WaitTimeoutException, IOException, HoyaException {
    int state = hoyaClient.waitForRoleInstanceLive(HBaseKeys.MASTER, spintime);
    return state == ClusterDescription.STATE_LIVE;
  }

  public static void dumpClusterStatus(HoyaClient hoyaClient, String text) {
    ClusterDescription status = hoyaClient.clusterDescription;
    dumpClusterDescription(text, status)
  }

  public static List<ClusterNode> listNodesInRole(HoyaClient hoyaClient, String role) {
    return hoyaClient.listClusterNodesInRole(role)
  }

  public static void dumpClusterDescription(String text, ClusterDescription status) {
    describe(text)
    log.info(prettyPrint(status.toJsonString()))
  }

  /**
   * Fetch the current site config from the Hoya AM, from the 
   * <code>clientProperties</code> field of the ClusterDescription
   * @param hoyaClient client
   * @param clustername name of the cluster
   * @return the site config
   */
  public static Configuration fetchClientSiteConfig(HoyaClient hoyaClient) {
    ClusterDescription status = hoyaClient.clusterDescription;
    Configuration siteConf = new Configuration(false)
    status.clientProperties.each { String key, String val ->
      siteConf.set(key, val, "hoya cluster");
    }
    return siteConf;
  }


  public static String fetchWebPage(String url) {
    def client = new HttpClient(new MultiThreadedHttpConnectionManager());
    client.httpConnectionManager.params.connectionTimeout = 10000;
    GetMethod get = new GetMethod(url);

    get.followRedirects = true;
    int resultCode = client.executeMethod(get);
    String body = get.responseBodyAsString;
    return body;
  }
}
