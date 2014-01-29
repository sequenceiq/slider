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

package org.apache.hoya.yarn.api

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.fs.FileSystem as HadoopFS
import org.apache.hadoop.fs.Path
import org.apache.hoya.api.ClusterDescription
import org.apache.hoya.api.ClusterNode
import org.apache.hoya.api.RoleKeys
import org.apache.hoya.exceptions.BadConfigException
import org.apache.hoya.providers.hbase.HBaseKeys
import org.apache.hoya.tools.HoyaUtils
import org.apache.hoya.yarn.cluster.YarnMiniClusterTestBase
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 test CD serialization/deserialization to and from JSON
 */
@CompileStatic
@Slf4j

class TestClusterDescriptionMapping extends YarnMiniClusterTestBase {


  ClusterDescription createCD() {
    ClusterDescription cd = new ClusterDescription();
    cd.name = "test"
    cd.state = ClusterDescription.STATE_LIVE;
    cd.roles = [
        (HBaseKeys.ROLE_MASTER): [
            (RoleKeys.ROLE_INSTANCES): "1"
        ]
    ]
    ClusterNode node = new ClusterNode()
    node.name = "masternode"
    cd.createTime = System.currentTimeMillis()
    cd.options = ["opt": "1"]
    return cd;
  }

  ClusterDescription parse(String s) {
    try {
      return ClusterDescription.fromJson(s);
    } catch (IOException e) {
      log.info("exception parsing: \n" + s);
      throw e;
    }
  }

  ClusterDescription roundTrip(ClusterDescription src) {
    return parse(src.toJsonString());
  }

  @Test
  public void testJsonify() throws Throwable {
    log.info(createCD().toJsonString())
  }

  @Test
  public void testEmptyRoundTrip() throws Throwable {
    roundTrip(new ClusterDescription())
  }

  @Test
  public void testRTrip() throws Throwable {
    ClusterDescription original = createCD()
    ClusterDescription received = roundTrip(original)

    assert received.getDesiredInstanceCount(HBaseKeys.ROLE_MASTER, 0) == 1

  }

  @Test
  public void testSaveLoadToFileSystem() throws Throwable {
    ClusterDescription original = createCD()
    File file = new File("target/cluster.json");
    URI fileURI = file.toURI()
    Path path = new Path(fileURI.toString());
    HadoopFS fileSystem = HadoopFS.get(fileURI, HoyaUtils.createConfiguration())
    original.save(fileSystem, path, true)
    ClusterDescription received = ClusterDescription.load(fileSystem, path)
    assert received.getDesiredInstanceCount(HBaseKeys.ROLE_MASTER,0) == 1

  }

  @Test
  public void testRoleMapLookup() throws Throwable {
    ClusterDescription status = createCD()

    assert null == status.getRole("undefined");
    assert null != status.getOrAddRole("undefined");
  }

  @Test
  public void testGetMandatory() {
    ClusterDescription cd = createCD()
    try {
      cd.getMandatoryOption("none")
      assert false;
    } catch (BadConfigException expected) {
      // expected

    }
    try {
      cd.getMandatoryRole(HBaseKeys.ROLE_WORKER)
      assert false;
    } catch (BadConfigException expected) {
      // expected
    }

    cd.getMandatoryOption("opt")
    cd.getMandatoryRole(HBaseKeys.ROLE_MASTER)
  }

  @Test
  public void testRoleLimitMax() throws Throwable {
    ClusterDescription cd = createCD()
    cd.setRoleOpt(HBaseKeys.ROLE_MASTER,
                  RoleKeys.YARN_MEMORY,
                  RoleKeys.YARN_RESOURCE_MAX)
    int limit = cd.getRoleResourceRequirement(HBaseKeys.ROLE_MASTER,
                                              RoleKeys.YARN_MEMORY,
                                              256, 512)
    assert limit == 512
  }

  @Test
  public void testRoleLimitNormal() throws Throwable {
    ClusterDescription cd = createCD()
    cd.setRoleOpt(HBaseKeys.ROLE_MASTER,
                  RoleKeys.YARN_MEMORY,
                  128)
    int limit = cd.getRoleResourceRequirement(HBaseKeys.ROLE_MASTER,
                                              RoleKeys.YARN_MEMORY,
                                              256, 512)
    assert limit == 128
  }

  @Test
  public void testRoleLimitDefval() throws Throwable {
    ClusterDescription cd = createCD()
    int limit = cd.getRoleResourceRequirement(HBaseKeys.ROLE_MASTER,
                                              RoleKeys.YARN_MEMORY,
                                              256, 512)
    assert limit == 256
  }

}
