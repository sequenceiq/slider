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

package org.apache.hoya.core.build;

import org.apache.hadoop.util.VersionInfo;
import org.apache.hoya.core.conf.MapOperations;
import org.apache.hoya.tools.HoyaVersionInfo;

import java.util.Properties;

public class BuildHelper {
  /**
   * Add the cluster build information; this will include Hadoop details too
   * @param dest map to insert this too
   * @param prefix prefix for the build info
   */
  public static void addBuildInfo(MapOperations dest, String prefix) {

    Properties props = HoyaVersionInfo.loadVersionProperties();
    dest.put(prefix + "." + HoyaVersionInfo.APP_BUILD_INFO, props.getProperty(
      HoyaVersionInfo.APP_BUILD_INFO));
    dest.put(prefix + "." + HoyaVersionInfo.HADOOP_BUILD_INFO,
             props.getProperty(HoyaVersionInfo.HADOOP_BUILD_INFO));

    dest.put(prefix + "." + HoyaVersionInfo.HADOOP_DEPLOYED_INFO,
             VersionInfo.getBranch() + " @" + VersionInfo.getSrcChecksum());
  }
}
