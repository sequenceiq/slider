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

package org.apache.hadoop.hoya.yarn.client

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.DFSConfigKeys
import org.apache.hadoop.hoya.api.RoleKeys
import org.apache.hadoop.hoya.exceptions.BadCommandArgumentsException
import org.apache.hadoop.hoya.tools.HoyaUtils
import org.apache.hadoop.hoya.yarn.Arguments
import org.apache.hadoop.hoya.yarn.CommonArgs
import org.apache.hadoop.hoya.yarn.HoyaActions
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.junit.Assert
import org.junit.Assume
import org.junit.Test

/**
 * Test handling of common arguments, specifically how things get split up
 */
@CompileStatic
@Slf4j

class TestCommonArgParsing implements HoyaActions {

  @Test
  public void testCreateActionArgs() throws Throwable {
    ClientArgs clientArgs = createClientArgs([HoyaActions.ACTION_CREATE, 'cluster1'])
    assert clientArgs.clusterName == 'cluster1'
  }

  @Test
  public void testCreateFailsNoClustername() throws Throwable {
    assertParseFails([HoyaActions.ACTION_CREATE])

  }

  @Test
  public void testCreateFailsTwoClusternames() throws Throwable {
    assertParseFails([
        HoyaActions.ACTION_CREATE,
        "c1",
        "c2",
      ])
  }

  @Test
  public void testListNoClusternames() throws Throwable {
    ClientArgs clientArgs = createClientArgs([HoyaActions.ACTION_LIST])
    assert clientArgs.clusterName == null
  }

  @Test
  public void testList1Clustername() throws Throwable {
    ClientArgs clientArgs = createClientArgs([HoyaActions.ACTION_LIST, 'cluster1'])
    assert clientArgs.clusterName == 'cluster1'
  }

  @Test
  public void testListFailsTwoClusternames() throws Throwable {
    assertParseFails([
        ACTION_LIST,
        "c1",
        "c2",
      ])
  }

  @Test
  public void testDefinitions() throws Throwable {
    ClientArgs ca = createClientArgs([
        ACTION_CREATE,
        "clustername",
        "-D","yarn.resourcemanager.principal=yarn/server@LOCAL",
        "-D","dfs.datanode.kerberos.principal=hdfs/server@LOCAL",
    ])
    Configuration conf = new Configuration(false)
    ca.applyDefinitions(conf)
    assert ca.clusterName == "clustername"
    HoyaUtils.verifyPrincipalSet(conf, YarnConfiguration.RM_PRINCIPAL);
    HoyaUtils.verifyPrincipalSet(
        conf,
        DFSConfigKeys.DFS_DATANODE_USER_NAME_KEY);

  }

  @Test
  public void testActionComesAfterParseSingleArg() throws Throwable {
    ClientArgs ca = createClientArgs([
        Arguments.ARG_WAIT , "0", 
        ACTION_LIST,
    ])
  }
  
  @Test
  public void testActionComesAfterParseComplexArg() throws Throwable {
    Configuration conf = new Configuration(false)
    ClientArgs ca = createClientArgs([
        Arguments.ARG_SYSPROP,"syspropkey=syspropval",
        ACTION_LIST,
    ])
  }

  /**
   * Test a thaw command
   * @throws Throwable
   */
  @Test
  public void testComplexThaw() throws Throwable {
    Assume.assumeTrue("test disabled -split arguments broken", false)

    Configuration conf = new Configuration(false)
    ClientArgs ca = createClientArgs([
        "--manager", "ubuntu:8032", "--filesystem", "hdfs://ubuntu:9090",
        "--secure","-S","java.security.krb5.realm=LOCAL","-S", "java.security.krb5.kdc=ubuntu",
        "-D","yarn.resourcemanager.principal=yarn/ubuntu@LOCAL",
        "-D","namenode.resourcemanager.principal=hdfs/ubuntu@LOCAL",
        "thaw","cl1"    
    ])
    assert "cl1" == ca.clusterName
  }
  
  /**
   * Test a force kill command where the app comes at the end of the line
   * @throws Throwable
   * 
   */
  @Test
  public void testEmergencyKill() throws Throwable {
    Assume.assumeTrue("test disabled -split arguments broken", false)

    Configuration conf = new Configuration(false)
    String appId = "application_1381252124398_0013"
    ClientArgs ca = createClientArgs([
        ACTION_EMERGENCY_FORCE_KILL,
        "--manager", "ubuntu:8032",
        "--filesystem", "hdfs://ubuntu:9090",
        "--secure",
        "-S","java.security.krb5.realm=LOCAL",
        "-S", "java.security.krb5.kdc=ubuntu",
        "-D","yarn.resourcemanager.principal=yarn/ubuntu@LOCAL",
        "-D","namenode.resourcemanager.principal=hdfs/ubuntu@LOCAL",
        appId
    ])
    assert appId == ca.clusterName
    
  }
  
  private void assertParseFails(List argsList) {
    try {
      ClientArgs clientArgs = createClientArgs(argsList)
      Assert.fail("exected an exception, got $clientArgs")
    } catch (BadCommandArgumentsException ignored) {
      //expected
    }
  }
  
  /**
   * build and parse client args, after adding the base args list
   * @param argsList
   */
  public ClientArgs createClientArgs(List argsList) {
    def serviceArgs = new ClientArgs(argsList + baseArgs())
    serviceArgs.parse()
    serviceArgs.postProcess();
    serviceArgs
  }

  /**
   * build the list of base arguments for all operations
   * @return the base arguments
   */
  private def baseArgs() {
    return [
        Arguments.ARG_ZKHOSTS, "localhost",
        Arguments.ARG_ZKPORT, "8080",
    ]
  }


  @Test
  public void testSingleRoleArg() throws Throwable {
    ClientArgs clientArgs = createClientArgs([
        HoyaActions.ACTION_CREATE, 'cluster1',
        Arguments.ARG_ROLE,"master","5",
    ])
    def tuples = clientArgs.roleTuples;
    assert tuples.size() == 2;
    Map<String, String> roleMap = clientArgs.convertTupleListToMap("roles", tuples);
    assert roleMap["master"] == "5"
  }
  
  @Test
  public void testNoRoleArg() throws Throwable {
    ClientArgs clientArgs = createClientArgs([
        HoyaActions.ACTION_CREATE, 'cluster1',
    ])
    def tuples = clientArgs.roleTuples;
    Map<String, String> roleMap = clientArgs.convertTupleListToMap("roles", tuples);
    assert roleMap["master"] == null
  }
  
  
  @Test
  public void testMultiRoleArg() throws Throwable {
    ClientArgs clientArgs = createClientArgs([
        HoyaActions.ACTION_CREATE, 'cluster1',
        Arguments.ARG_ROLE,"master","1",
        Arguments.ARG_ROLE,"worker","2",
    ])
    def tuples = clientArgs.roleTuples;
    assert tuples.size() == 4;
    Map<String, String> roleMap = clientArgs.convertTupleListToMap("roles", tuples);
    assert roleMap["master"] == "1"
    assert roleMap["worker"] == "2"
  }
   
  @Test
  public void testDuplicateRole() throws Throwable {
    ClientArgs clientArgs = createClientArgs([
        HoyaActions.ACTION_CREATE, 'cluster1',
        Arguments.ARG_ROLE,"master","1",
        Arguments.ARG_ROLE,"master","2",
    ])
    def tuples = clientArgs.roleTuples;
    assert tuples.size() == 4;
    try {
      Map<String, String> roleMap = clientArgs.convertTupleListToMap("roles", tuples);
      Assert.fail("got a role map $roleMap not a failure");
    } catch (BadCommandArgumentsException expected) {
      assert expected.message.contains(ClientArgs.ERROR_DUPLICATE_ENTRY)
    }
  }
     
  @Test
  public void testOddRoleCount() throws Throwable {
    ClientArgs clientArgs = createClientArgs([
        HoyaActions.ACTION_CREATE, 'cluster1',
        Arguments.ARG_ROLE,"master","1",
        Arguments.ARG_ROLE,"master","2",
    ])
    List<String> tuples = clientArgs.roleTuples
    tuples += "loggers";
    assert tuples.size() == 5;
    try {
      Map<String, String> roleMap = clientArgs.convertTupleListToMap("roles", tuples);
      Assert.fail("got a role map $roleMap not a failure");
    } catch (BadCommandArgumentsException expected) {
      assert expected.message.contains(ClientArgs.ERROR_PARSE_FAILURE)
    }
  }

  /**
   * Create some role-opt client args, so that multiple tests can use it 
   * @return the args
   */
  public ClientArgs createRoleOptClientArgs() {
    ClientArgs clientArgs = createClientArgs([
        HoyaActions.ACTION_CREATE, 'cluster1',
        Arguments.ARG_ROLE, "master", "1",
        Arguments.ARG_ROLEOPT, "master", "cheese", "swiss",
        Arguments.ARG_ROLEOPT, "master", "env.CHEESE", "cheddar",
        Arguments.ARG_ROLEOPT, "master", RoleKeys.YARN_CORES, 3,

        Arguments.ARG_ROLE, "worker", "2",
        Arguments.ARG_ROLEOPT, "worker", RoleKeys.YARN_CORES, 2,
        Arguments.ARG_ROLEOPT, "worker", RoleKeys.JVM_HEAP, "65536",
        Arguments.ARG_ROLEOPT, "worker", "env.CHEESE", "stilton",
    ])
    return clientArgs
  }

  @Test
  public void testRoleOptionParse() throws Throwable {
    ClientArgs clientArgs = createRoleOptClientArgs()
    
    def tripleMaps = clientArgs.roleOptionMap
    def workerOpts = tripleMaps["worker"];
    assert workerOpts.size() == 3
    assert workerOpts[RoleKeys.YARN_CORES] == "2"
    assert workerOpts[RoleKeys.JVM_HEAP] == "65536"
    
    def masterOpts = tripleMaps["master"];
    assert masterOpts.size() == 3
    assert masterOpts[RoleKeys.YARN_CORES] == "3"

  }

  @Test
  public void testRoleOptionsMerge() throws Throwable {
    ClientArgs clientArgs = createRoleOptClientArgs()
    
    def roleOpts = clientArgs.roleOptionMap

    def clusterRoleMap = [
        "master":["cheese":"french"],
        "worker":["env.CHEESE":"french"]
    ];
    HoyaUtils.applyCommandLineOptsToRoleMap(clusterRoleMap, roleOpts);

    def masterOpts = clusterRoleMap["master"];
    assert masterOpts["cheese"] == "swiss"

    def workerOpts = clusterRoleMap["worker"];
    assert workerOpts["env.CHEESE"] == "stilton"
  }

  @Test
  public void testEnvVariableApply() throws Throwable {
    ClientArgs clientArgs = createRoleOptClientArgs()
    
    def roleOpts = clientArgs.roleOptionMap
    def clusterRoleMap = [
        "master": ["cheese": "french"],
        "worker": ["env.CHEESE": "french"]
    ];
    HoyaUtils.applyCommandLineOptsToRoleMap(clusterRoleMap, roleOpts);

    def workerOpts = clusterRoleMap["worker"];
    assert workerOpts["env.CHEESE"] == "stilton";

    Map<String, String> envmap = HoyaUtils.buildEnvMap(workerOpts);
    assert envmap["CHEESE"] == "stilton";

  }


}
