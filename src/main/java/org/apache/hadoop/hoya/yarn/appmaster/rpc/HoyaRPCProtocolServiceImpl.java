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

package org.apache.hadoop.hoya.yarn.appmaster.rpc;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import org.apache.hadoop.hoya.api.HoyaRPCProtocolProtos;

/**
 * Relay from Protobuf to internal RPC.
 * 
 * This is not (currently) implemented.
 */
public class HoyaRPCProtocolServiceImpl extends HoyaRPCProtocolProtos.HoyaRPCProtocolService {

  private HoyaRPCProtocolProtos.HoyaRPCProtocolService real;

  public HoyaRPCProtocolServiceImpl(HoyaRPCProtocolProtos.HoyaRPCProtocolService real) {
    this.real = real;
  }

  @Override
  public void stopCluster(RpcController controller,
                          HoyaRPCProtocolProtos.StopClusterRequestProto request,
                          RpcCallback<HoyaRPCProtocolProtos.StopClusterResponseProto> done) {
    
  }

  @Override
  public void flexCluster(RpcController controller,
                          HoyaRPCProtocolProtos.FlexClusterRequestProto request,
                          RpcCallback<HoyaRPCProtocolProtos.FlexClusterResponseProto> done) {

  }

  @Override
  public void getJSONClusterStatus(RpcController controller,
                                   HoyaRPCProtocolProtos.getJsonClusterStatusRequestProto request,
                                   RpcCallback<HoyaRPCProtocolProtos.getJsonClusterStatusResponseProto> done) {

  }

  @Override
  public void listNodeUUIDsByRole(RpcController controller,
                                  HoyaRPCProtocolProtos.ListNodeUUIDsByRoleRequestProto request,
                                  RpcCallback<HoyaRPCProtocolProtos.ListNodeUUIDsByRoleResponseProto> done) {

  }

  @Override
  public void getNode(RpcController controller,
                      HoyaRPCProtocolProtos.GetNodeRequestProto request,
                      RpcCallback<HoyaRPCProtocolProtos.GetNodeResponseProto> done) {

  }

  @Override
  public void getClusterNodes(RpcController controller,
                              HoyaRPCProtocolProtos.GetClusterNodesRequestProto request,
                              RpcCallback<HoyaRPCProtocolProtos.GetClusterNodesResponseProto> done) {

  }
}
