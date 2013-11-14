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

package org.apache.hadoop.hoya.providers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hoya.HoyaKeys;
import org.apache.hadoop.hoya.api.ClusterDescription;
import org.apache.hadoop.hoya.api.OptionKeys;
import org.apache.hadoop.hoya.api.RoleKeys;
import org.apache.hadoop.hoya.exceptions.BadCommandArgumentsException;
import org.apache.hadoop.hoya.exceptions.BadConfigException;
import org.apache.hadoop.hoya.tools.HoyaUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * this is a factoring out of methods handy for providers. It's bonded to a log at
 * construction time
 */
public class ProviderUtils implements RoleKeys {

  protected final Logger log;

  /**
   * Create an instace
   * @param log log directory to use -usually the provider
   */
  
  public ProviderUtils(Logger log) {
    this.log = log;
  }

  /**
   * Validate requested JVM heap settings with the role options, and
   * flag if the JVM heap requested is larger than the 
   * @param role role with both YARN and heap settings
   * @return the JVM heap
   * @throws BadConfigException if the config is invalid
   */
  public int validateAndGetJavaHeapSettings(Map<String, String> role,
                                            int defHeap)
    throws BadConfigException {
    int yarnRAM = validateAndGetYARNMemory(role);
    return HoyaUtils.getIntValue(role, RoleKeys.JVM_HEAP, defHeap, 0, yarnRAM);
  }

  public int validateAndGetYARNMemory(Map<String, String> role) throws
                                                             BadConfigException {
    return HoyaUtils.getIntValue(role, RoleKeys.YARN_MEMORY, 0, 0, -1);
  }


  /**
   * build the log directory
   * @return the log dir
   */
  public String getLogdir() throws IOException {
    String logdir = System.getenv("LOGDIR");
    if (logdir == null) {
      logdir =
        HoyaKeys.HOYA_TMP_LOGDIR_PREFIX + UserGroupInformation.getCurrentUser().getShortUserName();
    }
    return logdir;
  }


  /**
   * Validate the node count and heap size values of a node class 
   *
   * @param name node class name
   * @param count requested node count
   * @param min requested heap size
   * @param max
   * @throws BadCommandArgumentsException if the values are out of range
   */
  public void validateNodeCount(String name,
                                int count,
                                int min,
                                int max) throws BadCommandArgumentsException {
    if (count < min) {
      throw new BadCommandArgumentsException(
        "requested no of %s nodes: %d is below the minimum of %d", name, count,
        min);
    }
    if (max > 0 && count > max) {
      throw new BadCommandArgumentsException(
        "requested no of %s nodes: %d is above the maximum of %d", name, count,
        max);
    }
  }

  /**
   * copy all options beginning site. into the site.xml
   * @param clusterSpec cluster specification
   * @param sitexml map for XML file to build up
   */
  public void propagateSiteOptions(ClusterDescription clusterSpec,
                                    Map<String, String> sitexml) {
    Map<String, String> options = clusterSpec.options;
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(OptionKeys.SITE_XML_PREFIX)) {
        String envName = key.substring(OptionKeys.SITE_XML_PREFIX.length());
        if (!envName.isEmpty()) {
          sitexml.put(envName, entry.getValue());
        }
      }
    }
  }

  /**
   * Propagate an option from the cluster specification option map
   * to the site XML map, using the site key for the name
   * @param clusterSpec cluster specification
   * @param optionKey key in the option map
   * @param sitexml  map for XML file to build up
   * @param siteKey key to assign the value to in the site XML
   * @throws BadConfigException if the option is missing from the cluster spec
   */
  public void propagateOption(ClusterDescription clusterSpec,
                              String optionKey,
                              Map<String, String> sitexml,
                              String siteKey) throws BadConfigException {
    sitexml.put(siteKey, clusterSpec.getMandatoryOption(optionKey));
  }
  
  /**
   * Build the image dir. This path is relative and only valid at the far end
   * @param clusterSpec cluster spec
   * @param archiveSubdir subdir
   * @return a relative path to the tar
   */
  public File buildImageDir(ClusterDescription clusterSpec,
                                   String archiveSubdir) {
    File basedir;
    if (clusterSpec.isImagePathSet()) {
      basedir = new File(new File(HoyaKeys.LOCAL_TARBALL_INSTALL_SUBDIR),
                         archiveSubdir);
    } else {
      basedir = new File(clusterSpec.getApplicationHome());
    }
    return basedir;
  }

  public static String convertToAppRelativePath(File file) {
    return convertToAppRelativePath(file.getPath());
  }

  public static String convertToAppRelativePath(String path) {
    return ApplicationConstants.Environment.PWD.$() + "/" + path;
  }


  public static void validatePathReferencesLocalDir(String meaning, String path)
      throws BadConfigException {
    File file = new File(path);
    if (!file.exists()) {
      throw new BadConfigException("%s directory %s not found", meaning, file);
    }
    if (!file.isDirectory()) {
      throw new BadConfigException("%s is not a directory: %s", meaning, file);
    }
  }

  /**
   * verify that a config option is set
   * @param configuration config
   * @param key key
   * @return the value, in case it needs to be verified too
   * @throws BadConfigException if the key is missing
   */
  public String verifyOptionSet(Configuration configuration, String key,
                                       boolean allowEmpty) throws BadConfigException {
    String val = configuration.get(key);
    if (val == null) {
      throw new BadConfigException(
        "Required configuration option \"%s\" not defined ", key);
    }
    if (!allowEmpty && val.isEmpty()) {
      throw new BadConfigException(
        "Configuration option \"%s\" must not be empty", key);
    }
    return val;
  }

  /**
   * Verify that a keytab property is defined and refers to a non-empty file
   *
   * @param siteConf configuration
   * @param prop property to look for
   * @return the file referenced
   * @throws BadConfigException on a failure
   */
  public File verifyKeytabExists(Configuration siteConf, String prop) throws
                                                                      BadConfigException {
    String keytab = siteConf.get(prop);
    if (keytab == null) {
      throw new BadConfigException("Missing keytab property %s",
                                   prop);

    }
    File keytabFile = new File(keytab);
    if (!keytabFile.exists()) {
      throw new BadConfigException("Missing keytab file %s defined in %s",
                                   keytabFile,
                                   prop);
    }
    if (keytabFile.length() == 0 || !keytabFile.isFile()) {
      throw new BadConfigException("Invalid keytab file %s defined in %s",
                                   keytabFile,
                                   prop);
    }
    return keytabFile;
  }


  /**
   * Create a data directory, using the path and any options related to
   * permissions
   * @param cd cluster spec
   * @param conf configuration 
   * @return the path to the directory
   * @throws IOException trouble
   */
  public Path createDataDirectory(ClusterDescription cd,
                                   Configuration conf) throws IOException {
    Path path = new Path(cd.dataPath);
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    if (!fs.exists(path)) {
      log.info("Creating data directory {}", path);
      String parentOpts =
        cd.getOption(OptionKeys.HOYA_CLUSTER_DIRECTORY_PERMISSIONS,
                     OptionKeys.DEFAULT_HOYA_CLUSTER_DIRECTORY_PERMISSIONS);
      fs.mkdirs(path.getParent(), new FsPermission(parentOpts));
      String dataOpts =
        cd.getOption(OptionKeys.HOYA_DATA_DIRECTORY_PERMISSIONS,
                     OptionKeys.DEFAULT_HOYA_DATA_DIRECTORY_PERMISSIONS);
      fs.mkdirs(path,new FsPermission(dataOpts));
    }
    return path;
  }

  /**
   * get the user name
   * @return the user name
   */
  public String getUserName() throws IOException {
    return UserGroupInformation.getCurrentUser().getShortUserName();
  }
}
