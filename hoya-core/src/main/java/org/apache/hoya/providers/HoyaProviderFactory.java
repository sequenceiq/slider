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
import org.apache.hoya.HoyaKeys;
import org.apache.hoya.HoyaXmlConfKeys;
import org.apache.hoya.exceptions.BadClusterStateException;
import org.apache.hoya.exceptions.HoyaException;
import org.apache.hoya.providers.hbase.HBaseKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for factories
 */
public abstract class HoyaProviderFactory extends Configured {

  public static final String DEFAULT_CLUSTER_TYPE = HBaseKeys.PROVIDER_HBASE;
  
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

  public abstract AbstractClientProvider createClientProvider();

  public abstract ProviderService createServerProvider();

  /**
   * Create a provider for a specific application
   * @param application app
   * @return app instance
   * @throws HoyaException on any instantiation problem
   */
  public static HoyaProviderFactory createHoyaProviderFactory(String application) throws
                                                                                  HoyaException {
    Configuration conf = loadHoyaConfiguration();
    if (application == null) {
      application = DEFAULT_CLUSTER_TYPE;
    }
    String providerKey = String.format(HoyaXmlConfKeys.KEY_HOYA_PROVIDER, application);
    String classname = conf.getTrimmed(providerKey);
    if (classname == null) {
      throw new BadClusterStateException(PROVIDER_NOT_FOUND, application,
                                            providerKey);
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
      throw new BadClusterStateException(PROVIDER_NOT_LOADED,
                                            application,
                                            providerKey,
                                            classname);
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
    throw new BadClusterStateException(ex,
                              "Failed to create an instance of %s : %s",
                              providerClass,
                              ex);
  }

  public static Configuration loadHoyaConfiguration() {
    Configuration conf = new Configuration();
    conf.addResource(HoyaKeys.HOYA_XML);
    return conf;
  }
}
