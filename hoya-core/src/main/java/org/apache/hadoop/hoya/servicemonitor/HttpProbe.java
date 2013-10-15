/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hoya.servicemonitor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

public class HttpProbe extends Probe {
  private static final Log LOG = LogFactory.getLog(HttpProbe.class);

  private final URL url;
  private final int timeout;
  private final int min, max;


  public HttpProbe(URL url, int timeout, int min, int max, Configuration conf) throws IOException {
    super("Http probe of " + url + " [" + min + "-" + max + "]", conf);
    this.url = url;
    this.timeout = timeout;
    this.min = min;
    this.max = max;
  }

  public static HttpProbe createHttpProbe(Configuration conf) throws
                                                              IOException {
    String path = conf.get(
      WEB_PROBE_URL, WEB_PROBE_DEFAULT_URL);
    int min = conf.getInt(
      WEB_PROBE_MIN, WEB_PROBE_DEFAULT_CODE);
    int max = conf.getInt(
      WEB_PROBE_MAX, WEB_PROBE_DEFAULT_CODE);
    return new HttpProbe(new URL(path),
                         conf.getInt(
                           PORT_PROBE_CONNECT_TIMEOUT,
                           PORT_PROBE_CONNECT_TIMEOUT_DEFAULT),
                         min,
                         max,
                         conf);
  }

  @Override
  public ProbeStatus ping(boolean livePing) {
    ProbeStatus status = new ProbeStatus();
    HttpURLConnection connection = null;
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Fetching " + url + " with timeout " + timeout);
      }
      connection = (HttpURLConnection) url.openConnection();
      connection.setInstanceFollowRedirects(true);
      connection.setConnectTimeout(timeout);
      int rc = connection.getResponseCode();
      if (rc < min || rc > max) {
        String error = "Probe " + url + " error code: " + rc;
        LOG.info(error);
        status.fail(this,
                    new IOException(error));
      } else {
        status.succeed(this);
      }
    } catch (IOException e) {
      String error = "Probe " + url + " failed: " + e;
      LOG.info(error, e);
      status.fail(this,
                  new IOException(error, e));
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
    return status;
  }

}
