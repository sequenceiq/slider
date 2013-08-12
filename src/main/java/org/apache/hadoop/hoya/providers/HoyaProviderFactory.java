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
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hoya.HoyaExitCodes;
import org.apache.hadoop.hoya.HoyaKeys;
import org.apache.hadoop.hoya.exceptions.HoyaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for factories
 */
public abstract class HoyaProviderFactory extends Configured {
  protected static final Logger LOG =
    LoggerFactory.getLogger(HoyaProviderFactory.class);
  public static final String PROVIDER_NOT_FOUND =
    "Unable to find provider of Hoya application type %s, which should have its classname defined in the property %s";

  public static final String PROVIDER_NOT_LOADED=
    "Failed to load of Hoya provider %s, defined in the property %s - classname %s";

  public HoyaProviderFactory(Configuration conf) {
    super(conf);
  }

  protected HoyaProviderFactory() {
  }

  public abstract ClusterBuilder createBuilder();

  public abstract ClusterExecutor createDeployer();

  /**
   * Create a provider for a specific application
   * @param application app
   * @return app instance
   * @throws HoyaException on any instantiation problem
   */
  public static HoyaProviderFactory createHoyaProviderFactory(String application) throws
                                                                                  HoyaException {
    Configuration conf = loadHoyaConfiguration();
    String providerKey = String.format(HoyaKeys.HOYA_PROVIDER_KEY, application);
    String classname = conf.getTrimmed(providerKey);
    if (classname == null) {
      throw new HoyaException(HoyaExitCodes.EXIT_BAD_CONFIGURATION,
                              String.format(PROVIDER_NOT_FOUND, application,
                                            providerKey));
    }
    LOG.debug("Provider key {}: value {}", providerKey, classname);
    Class<?> providerClass;
    try {
      providerClass = Class.forName(classname, true,
                                    HoyaProviderFactory.class.getClassLoader());
    } catch (ClassNotFoundException e) {
      LOG.debug("Failed to load class " + classname, e);
      providerClass = null;
    }
    if (providerClass == null) {
      throw new HoyaException(HoyaExitCodes.EXIT_BAD_CONFIGURATION,
                              String.format(PROVIDER_NOT_LOADED,
                                            application,
                                            providerKey,
                                            classname));
    }
    Exception ex;
    try {
      HoyaProviderFactory providerFactory =
        (HoyaProviderFactory) providerClass.newInstance();
      providerFactory.setConf(conf);
      return providerFactory;
    } catch (InstantiationException e) {
      ex = e;
    } catch (IllegalAccessException e) {
      ex = e;
    } catch (Exception e) {
      ex = e;
    }
    //by here the operation failed and ex is set to the value 
    throw new HoyaException(HoyaExitCodes.EXIT_INTERNAL_ERROR,
                            String.format(
                              "Failed to create an instance of %s : %s",
                              providerClass.toString(), ex.toString()), ex);
  }

  public static Configuration loadHoyaConfiguration() {
    Configuration conf = new Configuration();
    conf.addResource(HoyaKeys.HOYA_XML);
    return conf;
  }
}
