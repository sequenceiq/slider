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

package org.apache.hoya.providers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hoya.api.ClusterDescription;
import org.apache.hoya.api.OptionKeys;
import org.apache.hoya.exceptions.BadConfigException;

import java.util.Map;

/**
 * An optional base class for providers
 */
public abstract class AbstractProviderCore extends Configured implements
                                                              ProviderCore {

  protected AbstractProviderCore(Configuration conf) {
    super(conf);
  }

  protected AbstractProviderCore() {
  }

  private void putSiteOpt(Map<String, String> options, String key, String val) {
    options.put(
      OptionKeys.SITE_XML_PREFIX + key, val);
  }

  /**
   * Propagate a key's value from the conf to the site, ca
   * @param sitexml
   * @param conf
   * @param srckey
   * @param destkey
   */
  private void propagate(Map<String, String> sitexml,
                         Configuration conf,
                         String srckey, String destkey) {
    String val = conf.get(srckey);
    if (val != null) {
      sitexml.put(destkey, val);
    }
  }

  protected void assignIfSet(Map<String, String> sitexml,
                             String prop,
                             ClusterDescription cd,
                             String role,
                             String key) throws BadConfigException {
    Map<String, String> map = cd.getMandatoryRole(role);

    String value = map.get(key);
    if (value != null) {
      sitexml.put(prop, value);
    }
  }
}
