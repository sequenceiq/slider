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
import org.apache.hoya.core.conf.ConfTreeOperations;
import org.apache.hoya.core.conf.MapOperations;
import org.apache.hoya.core.persist.ConfPersister;
import org.apache.hoya.core.persist.InstancePaths;
import org.apache.hoya.core.persist.LockAcquireFailedException;
import org.apache.hoya.core.persist.LockHeldAction;
import org.apache.hoya.exceptions.BadClusterStateException;
import org.apache.hoya.exceptions.BadConfigException;
import org.apache.hoya.exceptions.ErrorStrings;
import org.apache.hoya.exceptions.HoyaException;
import org.apache.hoya.tools.CoreFileSystem;
import org.apache.hoya.tools.HoyaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static org.apache.hoya.api.OptionKeys.*;

/**
 * Build up the instance of a cluster.
 */
public class InstanceBuilder {

  private final String clustername;
  private final Configuration conf;
  private final CoreFileSystem coreFS;
  private final InstancePaths instancePaths;
  private AggregateConf instanceDescription;

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

  public AggregateConf getInstanceDescription() {
    return instanceDescription;
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
   */
  public void init(
    Path configSrcDir,
    String provider,
    AggregateConf instanceConf) {


    this.instanceDescription = instanceConf;

    //internal is extended
    ConfTreeOperations internalOps = instanceConf.getInternalOperations();

    Map<String, Object> md = internalOps.getConfTree().metadata;
    long time = System.currentTimeMillis();
    md.put(StatusKeys.INFO_CREATE_TIME_HUMAN, HoyaUtils.toGMTString(time));
    md.put(StatusKeys.INFO_CREATE_TIME_MILLIS, Long.toString(time));

    MapOperations globalOptions = internalOps.getGlobalOptions();
    BuildHelper.addBuildMetadata(md, "create");
    HoyaUtils.setInfoTime(md,
                          StatusKeys.INFO_CREATE_TIME_HUMAN,
                          StatusKeys.INFO_CREATE_TIME_MILLIS,
                          System.currentTimeMillis());

    internalOps.set(INTERNAL_AM_TMP_DIR,
                    instancePaths.tmpPathAM.toUri());
    internalOps.set(INTERNAL_SNAPSHOT_CONF_PATH,
                    instancePaths.snapshotConfPath.toUri());
    internalOps.set(INTERNAL_GENERATED_CONF_PATH,
                    instancePaths.generatedConfPath.toUri());
    internalOps.set(INTERNAL_DATA_DIR_PATH,
                    instancePaths.dataPath.toUri());


    internalOps.set(OptionKeys.INTERNAL_PROVIDER_NAME, provider);
    internalOps.set(OptionKeys.APPLICATION_NAME, clustername);

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
        throw new BadConfigException(
          ErrorStrings.E_BOTH_IMAGE_AND_HOME_DIR_SPECIFIED);
      }
      instanceDescription.getInternalOperations().set(INTERNAL_APPLICATION_IMAGE_PATH,
                                               appImage.toUri());
    } else {
      // the alternative is app home, which now MUST be set
      if (appHomeUnset) {
        // both args have been set
        throw new BadConfigException(ErrorStrings.E_NO_IMAGE_OR_HOME_DIR_SPECIFIED);
          
      }
      instanceDescription.getInternalOperations().set(INTERNAL_APPLICATION_HOME,
                                               appHomeDir);

    }
  }

  /**
   * Propagate any critical principals from the current site config down to the HBase one.
   */
  public void propagatePrincipals() {
    String dfsPrincipal = conf.get(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY);
    if (dfsPrincipal != null) {
      String siteDfsPrincipal = OptionKeys.SITE_XML_PREFIX +
                                DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY;
      instanceDescription.getAppConfOperations().set(siteDfsPrincipal, dfsPrincipal);
    }
  }

  public void propagateFilename() {
    String fsDefaultName = conf.get(
      CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY);
    instanceDescription.getAppConfOperations().set(OptionKeys.SITE_XML_PREFIX +
                                            CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
                                            fsDefaultName
                                           );

    instanceDescription.getAppConfOperations().set(OptionKeys.SITE_XML_PREFIX +
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
   * @param appconfdir
   */
  public void persist(Path appconfdir) throws
                                       IOException,
                                       HoyaException,
                                       LockAcquireFailedException {
    coreFS.createClusterDirectories(instancePaths);
    ConfPersister persister =
      new ConfPersister(coreFS, getInstanceDir());
    ConfDirSnapshotAction action = null;
    if (appconfdir != null) {
      action = new ConfDirSnapshotAction(appconfdir);
    }
    persister.save(instanceDescription, action);
  }

  /**
   * Add the ZK paths to the application options. 
   * This is skipped if the zkhosts are not set
   * @param zkhosts
   * @param zookeeperRoot
   * @param zkport
   */
  public void addZKPaths(String zkhosts,
                         String zookeeperRoot,
                         int zkport) {
    if (HoyaUtils.isSet(zkhosts)) {
      MapOperations globalAppOptions =
        instanceDescription.getAppConfOperations().getGlobalOptions();
      MapOperations globalInternalOptions =
        instanceDescription.getAppConfOperations().getGlobalOptions();
      globalAppOptions.put(ZOOKEEPER_PATH, zookeeperRoot);
      globalAppOptions.put(ZOOKEEPER_HOSTS, zkhosts);
      globalAppOptions.put(ZOOKEEPER_PORT, Integer.toString(zkport));
/* commented out unless/until we need ZK internally
     globalInternalOptions.put(ZOOKEEPER_PATH, zookeeperRoot);
      globalInternalOptions.put(ZOOKEEPER_HOSTS, zkhosts);
      globalInternalOptions.put(ZOOKEEPER_PORT, Integer.toString(zkport));*/
    }
  }


  /**
   * Class to execute the snapshotting of the configuration directory
   * while the persistence lock is held. 
   * 
   * This guarantees that there won't be an attempt to launch a cluster
   * until the snapshot is complete -as the write lock won't be released
   * until afterwards.
   */
  private class ConfDirSnapshotAction implements LockHeldAction {

    private final Path appconfdir;

    private ConfDirSnapshotAction(Path appconfdir) {
      this.appconfdir = appconfdir;
    }

    @Override
    public void execute() throws IOException, HoyaException {

      takeSnapshotOfConfDir(appconfdir);
    }
  }
  
}
