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

package org.apache.hadoop.hoya.yarn.utils

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.fs.FileSystem as HadoopFS
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hoya.api.ClusterDescription
import org.apache.hadoop.hoya.tools.HoyaUtils
import org.apache.hadoop.hoya.yarn.cluster.YarnMiniClusterTestBase
import org.junit.Before
import org.junit.Test

/**
 * Test that the cluster spec can be updated
 */
@Slf4j
@CompileStatic
class TestClusterSpecUpdate extends YarnMiniClusterTestBase {
  private Path testDirPath
  private URI testDirURI
  private File testDir
  private HadoopFS localFS

  @Before
  public void setup() {
    super.setup()
    testDir = new File("target/testClusterSpecUpdate")
    HoyaUtils.deleteDirectoryTree(testDir)
    testDirURI = testDir.toURI()
    testDirPath = new Path(testDirURI)
    localFS = HadoopFS.get(testDirURI, createConfiguration())
  }

  @Test
  public void testUpdateClusterSpec() throws Throwable {
    ClusterDescription clusterSpec = new ClusterDescription()
    clusterSpec.name = "before";
    Path specPath = new Path(testDirPath,"testUpdateClusterSpec.json")
    clusterSpec.save(localFS, specPath, true)
    clusterSpec.name = "updated"
    assert HoyaUtils.updateClusterSpecification(localFS, testDirPath, specPath, clusterSpec)
    ClusterDescription updatedSpec = ClusterDescription.load(localFS, specPath)
    assert "updated" == updatedSpec.name
  }
  
  @Test
  public void testUpdateNonexistentClusterSpec() throws Throwable {
    ClusterDescription clusterSpec = new ClusterDescription()
    clusterSpec.name = "nonexistent";
    Path specPath = new Path(testDirPath,"testUpdateNonexistentClusterSpec.json")
    assert HoyaUtils.updateClusterSpecification(localFS, testDirPath, specPath, clusterSpec)
    ClusterDescription updatedSpec = ClusterDescription.load(localFS, specPath)
    assert "nonexistent" == updatedSpec.name
  }
  
  
}
