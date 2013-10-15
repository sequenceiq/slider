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
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Probe for a port being open
 */
public class PortProbe extends Probe {
  private static final Log LOG = LogFactory.getLog(PortProbe.class);

  private String host;
  private int port;
  private int timeout;

  public PortProbe(String host, int port, int timeout, String name, Configuration conf)
      throws IOException {
    super("Port probe " + name + " " + host + ":" + port + " for " + timeout + "ms",
          conf);
    this.host = host;
    this.port = port;
    this.timeout = timeout;
  }

  public static PortProbe createPortProbe(Configuration conf,
                                          String hostname,
                                          int port) throws IOException {
    PortProbe portProbe = new PortProbe(hostname,
                                        port,
                                        conf.getInt(
                                          PORT_PROBE_CONNECT_TIMEOUT,
                                          PORT_PROBE_CONNECT_TIMEOUT_DEFAULT),
                                        "",
                                        conf);

    return portProbe;
  }

  @Override
  public void init() throws IOException {
    if (port >= 65536) {
      throw new IOException("Port is out of range: " + port);
    }
    InetAddress target;
    if (host != null) {
      LOG.debug("looking up host " + host);
      target = InetAddress.getByName(host);
    } else {
      LOG.debug("Host is null, retrieving localhost address");
      target = InetAddress.getLocalHost();
    }
    LOG.info("Checking " + target + ":" + port);
  }

  /**
   * Try to connect to the (host,port); a failure to connect within
   * the specified timeout is a failure
   * @param livePing is the ping live: true for live; false for boot time
   * @return the outcome
   */
  @Override
  public ProbeStatus ping(boolean livePing) {
    ProbeStatus status = new ProbeStatus();
    InetSocketAddress sockAddr = new InetSocketAddress(host, port);
    Socket socket = new Socket();
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Connecting to " + sockAddr.toString() + " connection-timeout=" +
                  MonitorUtils.millisToHumanTime(timeout));
      }
      socket.connect(sockAddr, timeout);
      status.succeed(this);
    } catch (IOException e) {
      String error = "Probe " + sockAddr + " failed: " + e;
      LOG.debug(error, e);
      status.fail(this,
                  new IOException(error, e));
    } finally {
      IOUtils.closeSocket(socket);
    }
    return status;

  }
}
