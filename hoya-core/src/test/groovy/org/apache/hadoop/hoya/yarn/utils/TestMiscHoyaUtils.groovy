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

package org.apache.hadoop.hoya.yarn.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hoya.tools.HoyaUtils
import org.apache.hadoop.hoya.yarn.HoyaTestBase
import org.junit.Test
import org.apache.hadoop.fs.FileSystem as HadoopFS

class TestMiscHoyaUtils extends HoyaTestBase {


  public static final String CLUSTER1 = "cluster1"

  @Test
  public void testPurgeTempDir() throws Throwable {
    //HoyaUtils. //

    HadoopFS fs = HadoopFS.get(new URI("file:///"), new Configuration())
    Path inst = HoyaUtils.createHoyaAppInstanceTempPath(fs, CLUSTER1, "001")
    assert fs.exists(inst)
    HoyaUtils.purgeHoyaAppInstanceTempFiles(fs, CLUSTER1)
    assert !fs.exists(inst)
  }
}
