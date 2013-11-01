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

package org.apache.hadoop.hoya.yarn.model.mock

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hoya.yarn.HoyaTestBase
import org.apache.hadoop.hoya.yarn.appmaster.state.AppState
import org.apache.hadoop.hoya.yarn.appmaster.state.ContainerAssignment
import org.apache.hadoop.hoya.yarn.appmaster.state.RoleInstance
import org.apache.hadoop.hoya.yarn.appmaster.state.RoleStatus
import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.junit.Before
import org.apache.hadoop.fs.FileSystem as HadoopFS

@CompileStatic
@Slf4j
abstract class BaseMockAppStateTest extends HoyaTestBase implements MockRoles {
  MockFactory factory = new MockFactory()
  AppState appState
  MockYarnEngine engine = new MockYarnEngine()
  protected HadoopFS fs;

  @Override
  void setup() {
    super.setup()
    YarnConfiguration conf = createConfiguration()
    fs = HadoopFS.get(new URI("file:///"), conf)
  }

  @Before
  void initApp(){

    String historyDirName = testName;
    appState = new AppState(new MockRecordFactory())
    appState.setContainerLimits(4096,64)
    
    YarnConfiguration conf = createConfiguration()
    fs = HadoopFS.get(new URI("file:///"), conf)
    File targetWorkDir = new File("target", historyDirName);
    Path targetHistoryPath = new Path(targetWorkDir.toURI())
//    fs.delete(targetHistoryPath, true)
    appState.buildInstance(factory.newClusterSpec(0, 0, 0),
                           new Configuration(false),
                           factory.ROLES, fs, targetHistoryPath)
  }

  abstract String getTestName();

  public RoleStatus getRole1Status() {
    return appState.lookupRoleStatus(ROLE1)
  }

  public RoleStatus getRole2Status() {
    return appState.lookupRoleStatus(ROLE2)
  }

  public RoleStatus getRole3Status() {
    return appState.lookupRoleStatus(ROLE3)
  }

  /**
   * Build a role instance from a container assignment
   * @param assigned
   * @return
   */
  RoleInstance buildInstance(ContainerAssignment assigned) {
    Container target = assigned.container
    RoleInstance ri = new RoleInstance(target)
    ri.buildUUID();
    ri.roleId = assigned.role.priority
    return ri
  }
}
