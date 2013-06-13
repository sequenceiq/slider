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
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.FileSystem as HadoopFS
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.io.IOUtils

/**
 * Methods to aid in config, both in the Configuration class and
 * with other parts of setting up Hoya-initated processes
 */
@CompileStatic
@Commons
class ConfigHelper {

  public static final FsPermission CONF_DIR_PERMISSION = new FsPermission(FsAction.ALL,
                                                                          FsAction.READ_EXECUTE,
                                                                          FsAction.NONE)

  public static def setConfigEntry(Configuration self, def key, def value) {
    self.set(key.toString(), value.toString())
  }

  public static String getConfigEntry(Configuration self, def key) {
    self.get(key.toString())
  }

  /**
   * Set an entire map full of values
   * @param map map
   * @return nothing
   */
  public static void addConfigMap(Configuration self, Map map) {
    map.each { Map.Entry mapEntry ->
      setConfigEntry(self,
                     mapEntry.key.toString(),
                     mapEntry.value.toString())  
    }
  }
  
  public static Path generateConfigDir(Configuration conf, String appId, Path outputDirectory) {
    
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
                                    String filename) {
    HadoopFS fs = HadoopFS.get(confdir.toUri(), systemConf);

    Path destPath = new Path(confdir, filename)
    FSDataOutputStream fos = fs.create(destPath);
    try {
      generatingConf.writeXml(fos);
    } finally {
      IOUtils.closeStream(fos);
    }
    return destPath
  }
  
  /**
   * Generate a config file in a destination directory on the local filesystem
   * @param confdir the directory path where the file is to go
   * @param filename the filename
   * @return the destination path
   */
  public static File generateConfig(Configuration generatingConf,
                                    File confdir,
                                    String filename) {
    

    File destPath = new File(confdir, filename)
    OutputStream fos = new FileOutputStream(destPath)
    try {
      generatingConf.writeXml(fos);
    } finally {
      IOUtils.closeStream(fos);
    }
    return destPath
  }

  /**
   * Take a list of definitions for HBase and create [-D,name=value] 
   * entries on a list, ready for appending to the command list
   * @param properties properties
   * @return
   */
  public static List<String> buildHadoopCommandLineDefinitions(String prefix, Map<String, String> properties) {
    List<String> definitions = []
    properties.each { String k, String v ->
      definitions << "-D" << "${k}=${v}".toString()
    }
    return definitions
  }


  public static String build_JVM_opts(Map<String, String> properties) {
    StringBuilder builder = new StringBuilder()
    properties.each { String k, String v ->
      builder << "-D${k}=${v}".toString() << ' '
    }
    return builder.toString();
  }

  /**
   * build a list of export strings for bash to handle as export name=value.
   * If this is to be handed to a bash -v command, the env vars still
   * need to be joined into a single line
   * @param properties env variables to build up
   * @return the export commands.
   */
  public static List<String> buildBashExportCommands(Map<String, String> properties) {
    List<String> definitions = []
    properties.each { String k, String v ->
      definitions << "export ${k}=\"${v}\"".toString()
    }
    return definitions
  }

  /**
   * Sanity check: make sure everything in the list
   * really is a string.
   * @param list of string objects
   */
  public static void verifyAllStringType(List list) {
    list.each { k ->
      if (!(k instanceof String)) {
        throw new IllegalArgumentException("${k} is not a string")
      }
    }
  }
  
  /**
   * looks for the config under confdir/templateFile; if not there
   * loads it from /conf/templateFile . 
   */
  public static Configuration loadTemplateConfiguration(File confDir,
                                                 String templateFilename,
                                                 String resource) {
    File templateFile = new File(confDir, templateFilename)
    Configuration conf = new Configuration(false)
    if (templateFile.exists()) {
      log.debug("Loading template $templateFile");
      conf.addResource(new FileInputStream(templateFile));
    } else {
      log.debug("Template  file $templateFile not found" +
                " -reverting to classpath resource $resource");
      conf.addResource(resource)
    }
    return conf
  }

}
