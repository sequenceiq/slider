package org.apache.hoya.yarn.appmaster.web.rest.agent;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 *
 *
 * Data model for agent heartbeat for server (ambari or app master).
 *
 */

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class HeartBeat {
  private long responseId = -1;
  private long timestamp;
  private String hostname;
  List<CommandReport> reports = new ArrayList<CommandReport>();
  List<ComponentStatus> componentStatus = new ArrayList<ComponentStatus>();
  private List<DiskInfo> mounts = new ArrayList<DiskInfo>();
  HostStatus nodeStatus;
  private AgentEnv agentEnv = null;

  public long getResponseId() {
    return responseId;
  }

  public void setResponseId(long responseId) {
    this.responseId=responseId;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public String getHostname() {
    return hostname;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  @JsonProperty("reports")
  public List<CommandReport> getReports() {
    return this.reports;
  }

  @JsonProperty("reports")
  public void setReports(List<CommandReport> reports) {
    this.reports = reports;
  }

  public HostStatus getNodeStatus() {
    return nodeStatus;
  }

  public void setNodeStatus(HostStatus nodeStatus) {
    this.nodeStatus = nodeStatus;
  }

  public AgentEnv getAgentEnv() {
    return agentEnv;
  }

  public void setAgentEnv(AgentEnv env) {
    agentEnv = env;
  }

  @JsonProperty("componentStatus")
  public List<ComponentStatus> getComponentStatus() {
    return componentStatus;
  }

  @JsonProperty("componentStatus")
  public void setComponentStatus(List<ComponentStatus> componentStatus) {
    this.componentStatus = componentStatus;
  }

  @JsonProperty("mounts")
  public List<DiskInfo> getMounts() {
    return this.mounts;
  }

  @JsonProperty("mounts")
  public void setMounts(List<DiskInfo> mounts) {
    this.mounts = mounts;
  }

  @Override
  public String toString() {
    return "HeartBeat{" +
           "responseId=" + responseId +
           ", timestamp=" + timestamp +
           ", hostname='" + hostname + '\'' +
           ", reports=" + reports +
           ", componentStatus=" + componentStatus +
           ", nodeStatus=" + nodeStatus +
           '}';
  }
}
