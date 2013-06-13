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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.fs.*

/**
 * Methods to aid in config, both in the Configuration class and
 * with other parts of setting up Hoya-initated processes
 */
@CompileStatic

class ConfigHelper {

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
  public static Configuration generateConfig(Map map, String appId, Path outputDirectory) {
    Configuration conf = HBaseConfiguration.create();
    addConfigMap(conf, map)
    FSDataOutputStream fos = FileSystem.get(conf).create(new Path(outputDirectory, appId+"/hbase-site.xml"));
    conf.writeXml(fos);
    fos.close();
    return conf
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
}
