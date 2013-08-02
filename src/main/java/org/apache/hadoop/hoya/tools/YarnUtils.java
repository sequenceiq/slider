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

package org.apache.hadoop.hoya.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;

public class YarnUtils {
  private static final Logger log = LoggerFactory.getLogger(YarnUtils.class);

  /**
   * Create an AM resource from the 
   * @param hdfs HDFS or other filesystem in use
   * @param destPath dest path in filesystem
   * @param resourceType resource type
   * @return the resource set up wih application-level visibility and the
   * timestamp & size set from the file stats.
   */
  public static LocalResource createAmResource(FileSystem hdfs,
                                               Path destPath,
                                               LocalResourceType resourceType) throws
                                                                               IOException {
    FileStatus destStatus = hdfs.getFileStatus(destPath);
    LocalResource amResource = Records.newRecord(LocalResource.class);
    amResource.setType(resourceType);
    // Set visibility of the resource 
    // Setting to most private option
    amResource.setVisibility(LocalResourceVisibility.APPLICATION);
    // Set the resource to be copied over
    amResource.setResource(ConverterUtils.getYarnUrlFromPath(destPath));
    // Set timestamp and length of file so that the framework 
    // can do basic sanity checks for the local resource 
    // after it has been copied over to ensure it is the same 
    // resource the client intended to use with the application
    amResource.setTimestamp(destStatus.getModificationTime());
    amResource.setSize(destStatus.getLen());
    return amResource;
  }

  public static InetSocketAddress getRmAddress(Configuration conf) {
    return conf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
                              YarnConfiguration.DEFAULT_RM_ADDRESS,
                              YarnConfiguration.DEFAULT_RM_PORT);
  }

  public static InetSocketAddress getRmSchedulerAddress(Configuration conf) {
    return conf.getSocketAddr(YarnConfiguration.RM_SCHEDULER_ADDRESS,
                              YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
                              YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
  }

  /**
   * probe to see if the RM scheduler is defined
   * @param conf config
   * @return true if the RM scheduler address is set to
   * something other than 0.0.0.0
   */
  public static boolean isRmSchedulerAddressDefined(Configuration conf) {
    InetSocketAddress address = getRmSchedulerAddress(conf);
    return isAddressDefined(address);
  }

  /**
   * probe to see if the address
   * @param address
   * @return true if the scheduler address is set to
   * something other than 0.0.0.0
   */
  public static boolean isAddressDefined(InetSocketAddress address) {
    return !(address.getHostName().equals("0.0.0.0"));
  }

  public static void setRmAddress(Configuration conf, String rmAddr) {
    conf.set(YarnConfiguration.RM_ADDRESS, rmAddr);
  }

  public static void setRmSchedulerAddress(Configuration conf, String rmAddr) {
    conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, rmAddr);
  }

  public static boolean hasAppFinished(ApplicationReport report) {
    return report == null ||
           report.getYarnApplicationState().ordinal() >= YarnApplicationState.FINISHED.ordinal();
  }

  public static String reportToString(ApplicationReport report) {
    if (report == null) {
      return "Null application report";
    }

    return "App " + report.getName() + "/" + report.getApplicationType() +
           "# " +
           report.getApplicationId() + " user " + report.getUser() +
           " is in state " + report.getYarnApplicationState() +
           "RPC: " + report.getHost() + ":" + report.getRpcPort();
  }

  /**
   * Register all files under a fs path as a directory to push out 
   * @param clusterFS cluster filesystem
   * @param srcDir src dir
   * @param destRelativeDir dest dir (no trailing /)
   * @return the list of entries
   */
  public static Map<String, LocalResource> submitDirectory(FileSystem clusterFS,
                                                           Path srcDir,
                                                           String destRelativeDir) throws
                                                                                   IOException {
    //now register each of the files in the directory to be
    //copied to the destination
    FileStatus[] fileset = clusterFS.listStatus(srcDir);
    Map<String, LocalResource> localResources =
      new HashMap<String, LocalResource>(fileset.length);
    for (FileStatus entry: fileset) {

      LocalResource resource = createAmResource(clusterFS,
                                                entry.getPath(),
                                                LocalResourceType.FILE);
      String relativePath = destRelativeDir + "/" + entry.getPath().getName();
      localResources.put(relativePath, resource);
    }
    return localResources;
  }


  public static String stringify(org.apache.hadoop.yarn.api.records.URL url) {
    return url.getScheme() + ":/" +
           (url.getHost() != null ? url.getHost() : "") + "/" + url.getFile();
  }

  public static int findFreePort(int start, int limit) {
    int found = 0;
    int port = start;
    int finish = start + limit;
    while (found==0 && port < finish) {
      if (isPortAvailable(port)) {
        found = port;
      } else {
        port++;
      }
    }
    return found;
  }

  /**
   * See if a port is available for listening on by trying to listen
   * on it and seeing if that works or fails.
   * @param port port to listen to
   * @return true if the port was available for listening on
   */
  public static boolean isPortAvailable(int port) {
    try {
      ServerSocket socket = new ServerSocket(port);
      socket.close();
      return true;
    } catch (IOException e) {
      return false;
    }
  }
}
