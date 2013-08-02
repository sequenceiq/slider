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

package org.apache.hadoop.hoya.tools

import groovy.transform.CompileStatic
import groovy.util.logging.Commons
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileSystem as HadoopFS
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.io.IOUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Methods to aid in config, both in the Configuration class and
 * with other parts of setting up Hoya-initated processes
 */
@CompileStatic
class ConfigHelper {
  private static final Logger log = LoggerFactory.getLogger(HoyaUtils.class);

  public static final FsPermission CONF_DIR_PERMISSION = new FsPermission(FsAction.ALL,
                                                                          FsAction.READ_EXECUTE,
                                                                          FsAction.NONE);

  /**
   * Dump the (sorted) configuration
   * @param conf config
   * @return the sorted keyset
   */
  public static TreeSet<String> dumpConf(Configuration conf) {
    TreeSet<String> keys = sortedConfigKeys(conf);
    keys.each { key ->
      log.info("$key={}",conf.get((String) key));
    }
    return keys;
  }

  public static TreeSet<String> sortedConfigKeys(Configuration conf) {
    TreeSet<String> sorted = new TreeSet<String>();
    conf.each { Map.Entry<String, String> entry ->
      sorted.add(entry.key);
    }
    return sorted;
  }


  /**
   * Set an entire map full of values
   * @param map map
   * @return nothing
   */
  public static void addConfigMap(Configuration self, Map map) {
    map.each { Map.Entry mapEntry ->
      self.set(mapEntry.key.toString(), mapEntry.value.toString())  
    }
  }


  public static Path generateConfigDir(Configuration conf, String appId, Path outputDirectory) throws IOException {
    
    Path confdir = new Path(outputDirectory, appId + "/conf");
    HadoopFS fs = HadoopFS.get(confdir.toUri(), conf);
    FsPermission perms = CONF_DIR_PERMISSION
    fs.mkdirs(confdir, perms)
    return confdir;
  }

  /**
   * Generate a config file in a destination directory on a given filesystem
   * @param systemConf system conf used for creating filesystems
   * @param confdir the directory path where the file is to go
   * @param filename the filename
   * @return the destination path
   */
  public static Path generateConfig(Configuration systemConf,
                                    Configuration generatingConf,
                                    Path confdir,
                                    String filename) throws IOException {
    HadoopFS fs = HadoopFS.get(confdir.toUri(), systemConf);
    Path destPath = new Path(confdir, filename)
    FSDataOutputStream fos = fs.create(destPath);
    try {
      generatingConf.writeXml(fos);
    } finally {
      IOUtils.closeStream(fos);
    }
    return destPath;
  }
  
  /**
   * Generate a config file in a destination directory on the local filesystem
   * @param confdir the directory path where the file is to go
   * @param filename the filename
   * @return the destination path
   */
  public static File generateConfig(Configuration generatingConf,
                                    File confdir,
                                    String filename) throws IOException{
    

    File destPath = new File(confdir, filename);
    OutputStream fos = new FileOutputStream(destPath);
    try {
      generatingConf.writeXml(fos);
    } finally {
      IOUtils.closeStream(fos);
    }
    return destPath;
  }

  public static Configuration loadConfFromFile(File file) {
    Configuration conf = new Configuration(false)
    conf.addResource(file.toURI().toURL());
    return conf;
  }
  
  /**
   * looks for the config under confdir/templateFile; if not there
   * loads it from /conf/templateFile . 
   */
  public static Configuration loadTemplateConfiguration(Configuration systemConf,
                                                        Path confdir,
                                                        String templateFilename,
                                                        String resource) {
    HadoopFS fs = HadoopFS.get(confdir.toUri(), systemConf);

    Path templatePath = new Path(confdir, templateFilename);
    Configuration conf = new Configuration(false);
    if (fs.exists(templatePath)) {
      log.debug("Loading template {}",templatePath);
      conf.addResource(templatePath);
    } else {
      log.debug("Template {} not found" +
                " -reverting to classpath resource {}", templatePath, resource);
      conf.addResource(resource);
    }
    return conf;
  }


}
