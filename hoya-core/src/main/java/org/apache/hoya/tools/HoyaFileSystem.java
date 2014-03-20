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

package org.apache.hoya.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hoya.HoyaKeys;
import org.apache.hoya.api.ClusterDescription;
import org.apache.hoya.core.persist.Filenames;
import org.apache.hoya.exceptions.BadClusterStateException;
import org.apache.hoya.exceptions.ErrorStrings;
import org.apache.hoya.exceptions.HoyaException;
import org.apache.hoya.exceptions.UnknownClusterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Extends Core Filesystem with operations to manipulate ClusterDescription
 * persistent state
 */
public class HoyaFileSystem extends CoreFileSystem {

  private static final Logger log = LoggerFactory.getLogger(HoyaFileSystem.class);

  public HoyaFileSystem(FileSystem fileSystem,
                        Configuration configuration) {
    super(fileSystem, configuration);
  }

  public HoyaFileSystem(Configuration configuration) throws IOException {
    super(configuration);
  }

  /**
   * Overwrite a cluster specification. This code
   * attempts to do this atomically by writing the updated specification
   * to a new file, renaming the original and then updating the original.
   * There's special handling for one case: the original file doesn't exist
   *
   * @param clusterSpec
   * @param clusterDirectory
   * @param clusterSpecPath
   * @return true if the original cluster specification was updated.
   */
  public boolean updateClusterSpecification(Path clusterDirectory,
                                            Path clusterSpecPath,
                                            ClusterDescription clusterSpec) throws IOException {

    //it is not currently there -do a write with overwrite disabled, so that if
    //it appears at this point this is picked up
    if (!fileSystem.exists(clusterSpecPath) &&
            writeSpecWithoutOverwriting(clusterSpecPath, clusterSpec)) {
      return true;
    }

    //save to a renamed version
    String specTimestampedFilename = "spec-" + System.currentTimeMillis();
    Path specSavePath =
            new Path(clusterDirectory, specTimestampedFilename + ".json");
    Path specOrigPath =
            new Path(clusterDirectory, specTimestampedFilename + "-orig.json");

    //roll the specification. The (atomic) rename may fail if there is
    //an overwrite, which is how we catch re-entrant calls to this
    if (!writeSpecWithoutOverwriting(specSavePath, clusterSpec)) {
      return false;
    }
    if (!fileSystem.rename(clusterSpecPath, specOrigPath)) {
      return false;
    }
    try {
      if (!fileSystem.rename(specSavePath, clusterSpecPath)) {
        return false;
      }
    } finally {
      if (!fileSystem.delete(specOrigPath, false)) {
        return false;
      }
    }
    return true;
  }

  public boolean writeSpecWithoutOverwriting(Path clusterSpecPath,
                                             ClusterDescription clusterSpec) {
    try {
      clusterSpec.save(fileSystem, clusterSpecPath, false);
    } catch (IOException e) {
      HoyaFileSystem.log.debug("Failed to save cluster specification -race condition? " + e,
              e);
      return false;
    }
    return true;
  }

  /**
   * Perform any post-load cluster validation. This may include loading
   * a provider and having it check it
   *
   * @param clusterSpecPath path to cspec
   * @param clusterSpec     the cluster spec to validate
   */
  public void verifySpecificationValidity(Path clusterSpecPath,
                                          ClusterDescription clusterSpec) throws HoyaException {
    if (clusterSpec.state == ClusterDescription.STATE_INCOMPLETE) {
      throw new BadClusterStateException(ErrorStrings.E_INCOMPLETE_CLUSTER_SPEC + clusterSpecPath);
    }
  }

  /**
   * Load a cluster spec then validate it
   *
   * @param clusterSpecPath path to cspec
   * @return the cluster spec
   * @throws java.io.IOException                      IO problems
   * @throws org.apache.hoya.exceptions.HoyaException cluster location, spec problems
   */
  public ClusterDescription loadAndValidateClusterSpec(Path clusterSpecPath) throws IOException, HoyaException {
    ClusterDescription clusterSpec =
            ClusterDescription.load(fileSystem, clusterSpecPath);
    //spec is loaded, just look at its state;
    verifySpecificationValidity(clusterSpecPath, clusterSpec);
    return clusterSpec;
  }

  /**
   * Locate a cluster specification in the FS. This includes a check to verify
   * that the file is there.
   *
   * @param clustername name of the cluster
   * @return the path to the spec.
   * @throws java.io.IOException                      IO problems
   * @throws org.apache.hoya.exceptions.HoyaException if the path isn't there
   */
  public Path locateClusterSpecification(String clustername) throws IOException, HoyaException {
    Path clusterDirectory = buildHoyaClusterDirPath(clustername);
    Path clusterSpecPath =
            new Path(clusterDirectory, HoyaKeys.CLUSTER_SPECIFICATION_FILE);
    verifyClusterSpecExists(clustername, clusterSpecPath);

    return clusterSpecPath;
  }

  /**
   * Locate an application conf json in the FS. This includes a check to verify
   * that the file is there.
   *
   * @param clustername name of the cluster
   * @return the path to the spec.
   * @throws IOException                      IO problems
   * @throws HoyaException if the path isn't there
   */
  public Path locateInstanceDefinition(String clustername) throws IOException, HoyaException {
    Path clusterDirectory = buildHoyaClusterDirPath(clustername);
    Path appConfPath =
            new Path(clusterDirectory, Filenames.APPCONF);
    verifyClusterSpecExists(clustername, appConfPath);
    return appConfPath;
  }


  /**
   * Verify that a cluster specification exists
   * @param clustername name of the cluster (For errors only)
   * @param fs filesystem
   * @param clusterSpecPath cluster specification path
   * @throws IOException IO problems
   * @throws HoyaException if the cluster specification is not present
   */
  public void verifyClusterSpecExists(String clustername,
                                             Path clusterSpecPath) throws
                                                                   IOException,
                                                                   HoyaException {
    if (!fileSystem.isFile(clusterSpecPath)) {
      log.debug("Missing specification file {}", clusterSpecPath);
      throw UnknownClusterException.unknownCluster(clustername
                             + "\n (definition not found at "
                             + clusterSpecPath);
    }
  }
}
