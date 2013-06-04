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

package org.apache.hadoop.hoya.tools

import groovy.transform.CompileStatic
import groovy.util.logging.Commons
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem as FS
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.yarn.api.records.LocalResource
import org.apache.hadoop.yarn.api.records.LocalResourceType
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.hadoop.yarn.util.Records

@Commons
@CompileStatic
class YarnUtils {

  /**
   * Create an AM resource from the 
   * @param hdfs HDFS or other filesystem in use
   * @param destPath dest path in filesystem
   * @param resourceType resource type
   * @return the resource set up wih application-level visibility and the
   * timestamp & size set from the file stats.
   */
  public static LocalResource createAmResource(FS hdfs,
                                        Path destPath,
                                        LocalResourceType resourceType) {
    FileStatus destStatus = hdfs.getFileStatus(destPath);
    LocalResource amResource = Records.newRecord(LocalResource.class);
    amResource.type = resourceType;
    // Set visibility of the resource 
    // Setting to most private option
    amResource.visibility = LocalResourceVisibility.APPLICATION;
    // Set the resource to be copied over
    amResource.resource = ConverterUtils.getYarnUrlFromPath(destPath);
    // Set timestamp and length of file so that the framework 
    // can do basic sanity checks for the local resource 
    // after it has been copied over to ensure it is the same 
    // resource the client intended to use with the application
    amResource.timestamp = destStatus.modificationTime;
    amResource.size = destStatus.len;
    return amResource;
  }

  public static InetSocketAddress getRmAddress(Configuration conf) {
    return conf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
                              YarnConfiguration.DEFAULT_RM_ADDRESS,
                              YarnConfiguration.DEFAULT_RM_PORT);
  }
  
  public static setRmAddress(Configuration conf, String rmAddr) {
    conf.set(YarnConfiguration.RM_ADDRESS, rmAddr);
  }
  public static setRmAddressGlobal(String rmAddr) {
    Configuration rmResource = new Configuration(false);
    setRmAddress(rmResource, rmAddr);
    File f = File.createTempFile("config","xml");
    FileOutputStream fof = new FileOutputStream(f);
    try {
      rmResource.writeXml(fof);
    } finally {
      IOUtils.closeStream(fof);
    }
    Configuration.addDefaultResource(f.getAbsolutePath());
    
  }
}
