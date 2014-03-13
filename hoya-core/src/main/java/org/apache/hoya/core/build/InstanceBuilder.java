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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hoya.HoyaXmlConfKeys;
import org.apache.hoya.api.OptionKeys;
import org.apache.hoya.api.StatusKeys;
import org.apache.hoya.core.conf.AggregateConf;
import org.apache.hoya.core.conf.ConfTree;
import org.apache.hoya.core.conf.ConfTreeOperations;
import org.apache.hoya.core.conf.MapOperations;
import org.apache.hoya.core.persist.ConfPersister;
import org.apache.hoya.core.persist.InstancePaths;
import org.apache.hoya.core.persist.LockAcquireFailedException;
import org.apache.hoya.exceptions.BadClusterStateException;
import org.apache.hoya.exceptions.BadConfigException;
import org.apache.hoya.exceptions.HoyaException;
import org.apache.hoya.tools.CoreFileSystem;
import org.apache.hoya.tools.HoyaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static org.apache.hoya.api.OptionKeys.INTERNAL_AM_TMP_DIR;
import static org.apache.hoya.api.OptionKeys.INTERNAL_APP_PACKAGE_PATH;
import static org.apache.hoya.api.OptionKeys.INTERNAL_GENERATED_CONF__PATH;
import static org.apache.hoya.api.OptionKeys.INTERNAL_SNAPSHOT_CONF_PATH;
import static org.apache.hoya.api.OptionKeys.ZOOKEEPER_HOSTS;
import static org.apache.hoya.api.OptionKeys.ZOOKEEPER_PATH;
import static org.apache.hoya.api.OptionKeys.ZOOKEEPER_PORT;

/**
 * Build up the instance of a cluster.
 */
public class InstanceBuilder {

  private final String clustername;
  private final Configuration conf;
  private final CoreFileSystem coreFS;
  private final InstancePaths instancePaths;
  private AggregateConf instanceConf;

  private static final Logger log =
    LoggerFactory.getLogger(InstanceBuilder.class);

  public InstanceBuilder(CoreFileSystem coreFileSystem,
                         Configuration conf,
                         String clustername) {
    this.clustername = clustername;
    this.conf = conf;
    this.coreFS = coreFileSystem;
    Path instanceDir = coreFileSystem.buildHoyaClusterDirPath(clustername);
    instancePaths = new InstancePaths(instanceDir);

  }

  public AggregateConf getInstanceConf() {
    return instanceConf;
  }

  public InstancePaths getInstancePaths() {
    return instancePaths;
  }


  @Override
  public String toString() {
    return "Builder working with " + clustername + " at " +
           getInstanceDir();
  }

  private Path getInstanceDir() {
    return instancePaths.instanceDir;
  }

  /**
   * Initial part of the build process
   * @param configSrcDir
   * @param provider
   * @param internal
   * @param resources
   * @param app_conf
   */
  public void init(
    Path configSrcDir,
    String provider,
    AggregateConf instanceConf) {


    this.instanceConf = instanceConf;

    //internal is extended
    ConfTreeOperations internalOps = instanceConf.getInternalOperations();

    Map<String, Object> md = internalOps.getConfTree().metadata;
    long time = System.currentTimeMillis();
    md.put(StatusKeys.INFO_CREATE_TIME_HUMAN, HoyaUtils.toGMTString(time));
    md.put(StatusKeys.INFO_CREATE_TIME_MILLIS, Long.toString(time));

    BuildHelper.addBuildInfo(internalOps.getGlobalOptions(), "create");

    internalOps.set(INTERNAL_AM_TMP_DIR,
                    instancePaths.tmpPathAM.toUri());
    internalOps.set(INTERNAL_SNAPSHOT_CONF_PATH,
                    instancePaths.snapshotConfPath.toUri());
    internalOps.set(INTERNAL_GENERATED_CONF__PATH,
                    instancePaths.generatedConfPath.toUri());
  }

  /**
   * Set up the image/app home path
   * @param appImage path in the DFS to the tar file
   * @param appHomeDir other strategy: home dir
   * @throws BadConfigException if both or neither are found (its an xor)
   */
  public void setImageDetails(
    Path appImage,
    String appHomeDir) throws BadConfigException {
    boolean appHomeUnset = HoyaUtils.isUnset(appHomeDir);
    // App home or image
    if (appImage != null) {
      if (!appHomeUnset) {
        // both args have been set
        throw new BadConfigException("Both application image path and home dir"
                                     + " have been provided");
      }
      instanceConf.getInternalOperations().set(INTERNAL_APP_PACKAGE_PATH,
                                               appImage.toUri());
    } else {
      // the alternative is app home, which now MUST be set
      if (appHomeUnset) {
        // both args have been set
        throw new BadConfigException(
          "No image path or home directory provided");
      }
      instanceConf.getInternalOperations().set(INTERNAL_APP_PACKAGE_PATH,
                                               appHomeDir);

    }
  }

  /**
   * Propagate any critical principals from the current site config down to the HBase one.
   * @param clusterSpec cluster spec
   * @param config config to read from
   */
  public void propagatePrincipals() {
    String dfsPrincipal = conf.get(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY);
    if (dfsPrincipal != null) {
      String siteDfsPrincipal = OptionKeys.SITE_XML_PREFIX +
                                DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY;
      instanceConf.getAppConfOperations().set(siteDfsPrincipal, dfsPrincipal);
    }
  }

  public void propagateFilename() {
    String fsDefaultName = conf.get(
      CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY);
    instanceConf.getAppConfOperations().set(OptionKeys.SITE_XML_PREFIX +
                                            CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
                                            fsDefaultName
                                           );

    instanceConf.getAppConfOperations().set(OptionKeys.SITE_XML_PREFIX +
                                            HoyaXmlConfKeys.FS_DEFAULT_NAME_CLASSIC,
                                            fsDefaultName
                                           );

  }


  public void takeSnapshotOfConfDir(Path appconfdir) throws
                                                     IOException,
                                                     BadConfigException,
                                                     BadClusterStateException {
    FileSystem srcFS = FileSystem.get(appconfdir.toUri(), conf);
    if (!srcFS.isDirectory(appconfdir)) {
      throw new BadConfigException(
        "Source Configuration directory is not valid: %s",
        appconfdir.toString());
    }
    // bulk copy
    FsPermission clusterPerms = coreFS.getInstanceDirectoryPermissions();
    // first the original from wherever to the DFS
    HoyaUtils.copyDirectory(conf, appconfdir, instancePaths.snapshotConfPath,
                            clusterPerms);
  }


  /**
   * Persist this
   * @throws IOException
   * @throws HoyaException
   * @throws LockAcquireFailedException
   */
  public void persist() throws
                        IOException,
                        HoyaException,
                        LockAcquireFailedException {
    coreFS.createClusterDirectories(instancePaths);
    ConfPersister persister =
      new ConfPersister(coreFS, getInstanceDir());
    persister.save(instanceConf);
  }

  /**
   * Add the ZK paths to the application options
   * @param zkhosts
   * @param zookeeperRoot
   * @param zkport
   */
  public void addZKPaths(String zkhosts,
                         String zookeeperRoot,
                         int zkport) {
    MapOperations globalAppOptions =
      instanceConf.getAppConfOperations().getGlobalOptions();
    globalAppOptions.put(ZOOKEEPER_PATH, zookeeperRoot);
    globalAppOptions.put(ZOOKEEPER_HOSTS, zkhosts);
    globalAppOptions.put(ZOOKEEPER_PORT, Integer.toString(zkport));
  }

  /**
   * Merge in g
   * @param options
   */
  public void mergeAppGlobalOptions(Map<String, String> options) {
    instanceConf.getAppConfOperations().getGlobalOptions().putAll(options);
  }
}
