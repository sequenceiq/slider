/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *   
 *    http://www.apache.org/licenses/LICENSE-2.0
 *   
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License. See accompanying LICENSE file.
 */

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

package org.apache.hadoop.hoya.yarn.appmaster.rpc;

import com.google.protobuf.BlockingService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hoya.api.HoyaClusterProtocol;
import org.apache.hadoop.hoya.api.proto.HoyaClusterAPI;
import org.apache.hadoop.hoya.yarn.appmaster.HoyaAppMaster;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.ProtocolProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcEngine;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

public class RpcBinder {
  protected static final Logger log =
    LoggerFactory.getLogger(HoyaAppMaster.class);

  public static Server createProtobufServer(InetSocketAddress addr,
                                            Configuration conf,
                                            SecretManager<? extends TokenIdentifier> secretManager,
                                            int numHandlers,
                                            BlockingService blockingService,
                                            String portRangeConfig) throws
                                                      IOException {
    Class<HoyaClusterProtocolPB> hoyaClusterAPIClass = registerHoyaAPI(
      conf);
    RPC.Server server = new RPC.Builder(conf).setProtocol(hoyaClusterAPIClass)
                                             .setInstance(blockingService)
                                             .setBindAddress(addr.getHostName())
                                             .setPort(addr.getPort())
                                             .setNumHandlers(numHandlers)
                                             .setVerbose(false)
                                             .setSecretManager(secretManager)
                                             .setPortRangeConfig(
                                               portRangeConfig)
                                             .build();
    log.debug(
      "Adding protocol " + hoyaClusterAPIClass.getCanonicalName() + " to the server");
    server.addProtocol(RPC.RpcKind.RPC_PROTOCOL_BUFFER, hoyaClusterAPIClass,
                       blockingService);
    return server;
  }

  public static Class<HoyaClusterProtocolPB> registerHoyaAPI(
    Configuration conf) {
    Class<HoyaClusterProtocolPB> hoyaClusterAPIClass =
      HoyaClusterProtocolPB.class;
    RPC.setProtocolEngine(conf, hoyaClusterAPIClass, ProtobufRpcEngine.class);
    
    //quick sanity check here
    assert verifyBondedToProtobuf(conf, hoyaClusterAPIClass); 
    
    return hoyaClusterAPIClass;
  }

  public static boolean verifyBondedToProtobuf(Configuration conf,
                                                Class<HoyaClusterProtocolPB> hoyaClusterAPIClass) {
    return conf.getClass("rpc.engine." + hoyaClusterAPIClass.getName(),
                         RpcEngine.class) .equals(ProtobufRpcEngine.class);
  }


  public static HoyaClusterProtocol connectToServer(InetSocketAddress addr,
                                                    UserGroupInformation currentUser,
                                                    Configuration conf,
                                                    int rpcTimeout) throws IOException {
    Class<HoyaClusterProtocolPB> hoyaClusterAPIClass = registerHoyaAPI(
      conf);

    log.debug("Connecting to Hoya Server at {}", addr);
    ProtocolProxy<HoyaClusterProtocolPB> protoProxy =
      RPC.getProtocolProxy(hoyaClusterAPIClass,
                           1,
                           addr,
                           currentUser,
                           conf,
                           NetUtils.getDefaultSocketFactory(conf),
                           rpcTimeout,
                           null);
    HoyaClusterProtocolPB endpoint = protoProxy.getProxy();
    return new HoyaClusterProtocolProxy(endpoint);
  }
  
  
  public static Server createClassicServer(HoyaClusterAPI impl, Configuration conf,
                                           SecretManager<? extends TokenIdentifier> secretManager,
                                           int numHandlers) throws
                                                                                    IOException {
    /*

    //classic RPC
    server = new RPC.Builder(getConfig())
      .setProtocol(HoyaClusterProtocol.class)
      .setInstance(this)
      .setPort(0)
      .setNumHandlers(5)
//        .setSecretManager(sm)
      .build();
    server.start();
*/
    //classic RPC
    Server server = new RPC.Builder(conf)
      .setProtocol(HoyaClusterProtocol.class)
      .setInstance(impl)
      .setPort(0)
      .setNumHandlers(numHandlers)
        .setSecretManager(secretManager)
      .build();
    server.start();
    return server;
  }
}
