<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
  
   http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->
  
# Role History: how Hoya brings back nodes in the same location

### 2013-09-24: WiP design doc

## Outstanding issues

1. How best to distinguish at thaw time from nodes used just before thawing
from nodes used some period before? Should the RoleHistory simply forget
about nodes which are older than some threshold when reading in the history?
1. Is there a way to avoid tracking the outstanding requests? 

## Introduction

Hoya tries to bring up instances of a given role on the machine(s) on which
they last ran -it remembers after flexing or cluster freeze which
servers were last used for a role, and persists this for use in restarts.

It does this in the assumption that the role instances prefer node-local
access to data previously persisted to HDFS. This is precisely the case
for Apache HBase, which can use Unix Domain Sockets to talk to the DataNode
without using the TCP stack. The HBase master persists to HDFS the tables
assigned to specific Region Servers, and when HBase is restarted its master
tries to reassign the same tables back to Region Servers on the same machine.

For this to work in a dynamic cluster, Hoya needs to bring up Region Servers
on the previously used hosts, so that the HBase Master can re-assign the same
tables.

### Terminology

* **Role Instance** : a single instance of a role.
* **Node** : A server in the YARN Physical (or potentially virtual) Cluster of servers.
* **Hoya Cluster**: The set of role instances deployed by Hoya so as to 
 create a single aggregate application.
* **Hoya AM**: The Application Master of Hoya: the program deployed by YARN to
manage its Hoya Cluster.
* **RM** YARN Resource Manager

### Assumptions

Here are some assumptions in Hoya's design


1. Instances of a specific role should preferably be deployed onto different
servers. This enables Hoya to only remember the set of server nodes onto
which instances were created, rather than more complex facts such as "two Region
Servers were previously running on Node #17. On restart Hoya can simply request
one instance of a Region Server on a specific node, leaving the other instance
to be arbitrarily deployed by YARN. This strategy should help reduce the *affinity*
in the role deployment, so increase their resilience to failure.

1. There is no need to make sophisticated choices on which nodes to request
re-assignment -such as recording the amount of data persisted by a previous
instance and prioritising nodes based on such data. More succinctly 'the
only priority needed when asking for nodes is *ask for the most recently used*.

1. Different roles are independent: it is not an issue if a role of one type
 (example, an Accumulo Monitor and an Accumulo Tablet Server) are on the same
 host. This assumption allows Hoya to only worry about affinity issues within
 a specific role, rather than across all roles.
 
1. After a cluster has been started, the rate of change of the cluster is
low: both node failures and cluster flexing happen at the rate of every few
hours, rather than every few seconds. This allows Hoya to avoid needing
data structures and layout persistence code designed for regular and repeated changes.

1. Instance placement is best-effort: if the previous placement cannot be satisfied,
the application will still perform adequately with role instances deployed
onto new servers. More specifically, if a previous server is unavailable
for hosting a role instance due to lack of capacity or availability, Hoya
will not decrement the number of instances to deploy: instead it will rely
on YARN to locate a new node -ideally on the same rack.

1. If two instances of the same role do get assigned to the same server, it
is not a failure condition.

1. Tracking failure statistics of nodes may be a feature to add in future;
designing the Role History datastructures to enable future collection
of rolling statistics on recent failures would be a first step to this 

### The Role History

The RoleHistory is a datastructure which models the role assignment, and
can persist it to and restore it from the (shared) filesystem.

* For each role, there is a list of nodes used in the past
* This history is used when selecting a node for a role
* This history remembers when nodes were allocated. These are re-requested
when thawing a cluster.
* It must also remember when nodes were released -these are re-requested
when returning the cluster size to a previous size during flex operations.
* It has to track nodes for which Hoya has an outstanding container request
with YARN. This ensures that the same node is not requested more than once
due to outstanding requests.
* It does not retain a complete history of the role -and does not need to.
All it needs to retain is the recent history for every node onto which a role
instance has been deployed. Specifically, the last allocation or release
operation on a a nodes is all that needs to be persisted. 

* On AM startup, all nodes are considered candidates, even those nodes marked
as active -as they were from the previous instance.
* During cluster flexing, nodes marked as released -and for which there is no
outstanding request - are considered candidates for requesting new instances.
* The AM picks the candidate nodes from the head of the time-ordered list.

### Persistence

The state of the role is persisted to HDFS on changes

1. When nodes are allocated, the Role History is marked as dirty
1. When container release callbacks are received, the Role History is marked as dirty
1. When nodes are requested or a release request made, the Role History is *not* marked as dirty. This
information is not relevant on AM restart.

As at startup a large number of allocations may arrive in a short period of time,
the Role History may be updated very rapidly -yet as the containers are
only recently activated, it is not likely that an immediately restarted Hoya
cluster would gain by re-requesting containers on them -their historical
value is more important than their immediate past.

Accordingly, the role history may be persisted to HDFS asynchronously, with
the dirty bit triggering an flushing of the state to HDFS. The datastructure
will still need to be synchronized for cross thread access, but the 
sync operation will not be a major deadlock, compared to saving the file on every
container allocation response (which will actually be the initial implementation).


## Weaknesses in this design

**Blacklisting**: even if a node fails repeatedly, this design will still try to re-request
instances on this node: there is no blacklisting. As a central blacklist
for YARN has been proposed, it is hoped that this issue will be addressed centrally,
without Hoya having to remember which nodes are unreliable *for that particular
Hoya cluster*.

**Anti-affinity**: If multiple role instances are assigned to the same node,
Hoya has to choose on restart or flexing whether to ask for multiple
nodes on that node again, or to pick other nodes.

**Bias towards recent nodes over most-used**: re-requesting the most
recent nodes, rather than those with the most history of use, may
push Hoya to requesting nodes that were only briefly in use -and so have
on a small amount of local state, over nodes that have had long-lived instances.
This is a problem that could perhaps be addressed by preserving more
history of a node -maintaining some kind of moving average of
node use and picking the heaviest used, or some other more-complex algorithm.
This may be possible, but we'd need evidence that the problem existed before
trying to address it.





# The NodeMap: the core of the Role History

The core data structure is a map of every node in the cluster tracking
how many containers are allocated to specific roles in it, and, when there
are no active instances, when it was last used. This history is used to
choose where to request new containers. Because of the asynchronous
allocation and release of containers, the Role History also needs to track
outstanding release requests --and, more critically, outstanding allocation
requests. If Hoya has already requested a container for a specific role
on a host, then asking for another container of that role would break
anti-affinity requirements. Note that not tracking oustanding requests would
radically simplify some aspects of the design, especially the complexity
of correlating allocation responses with the original requests -and so the
actual hosts originally requested.

1. Hoya builds up a map of which nodes have recently been used.
1. Every node counts the no. of active containers in each role.
1. Nodes are only chosen for allocation requests when there are no
active or requested containers on that node.
1. When choosing which instances to release, Hoya could pick the node with the
most containers on it. Ths would spread load.
1. When there are no empty nodes to request containers on, a random request would
let YARN choose.

Strengths
* Handles the multi-container on one node problem
* By storing details about every role, cross-role decisions could be possible
* Simple counters can track the state of pending add/release requests
* Scales well to a rapidly flexing cluster
* Simple to work with and persist
* Easy to view and debug
* Would support cross-role collection of node failures in future

Weaknesses
* Size of the data structure is O(nodes * role-instances). This
could be mitigated by regular cleansing of the structure. For example, at
thaw time (or intermittently) all unused nodes > 2 weeks old could be dropped.
* Locating a free node could take O(nodes) lookups -and if the criteria of "newest"
is included, will take exactly O(nodes) lookups. As an optimization, a list
of recently explicitly released nodes can be maintained.
* Need to track outstanding requests against nodes, so that if a request
was satisifed on a different node, the original node's request count is decremented
- *not that of the node actually allocated*.  

## Data Structures


### RoleHistory

    starttime: long
    saved: long
    dirty: boolean

    nodemap: NodeMap
    roles: RoleList
    
    
    relecentlyReleased[]: transient RecentlyReleasedList
    outstandingRequests: transient OutstandingRequestTracker

    
### NodeMap

    Map: NodeId -> Node
    
### RoleList

A list mapping role to int enum is needed to index NodeEntry elements in
the Node arrays. Although such an enum is already implemented in the Hoya
Providers, explicitly serializing and deserializing it would make
the persitent structure easier to parse in other tools, and resilient
to changes in the number or position of roles.

This list could also retain information about recently used/released nodes,
so that the selection of containers to request could shortcut a search


### Node

Every node is modeled as a ragged array of `NodeEntry` instances, indexed
by role index

    NodeEntry[roles]
    get(roleID): NodeEntry
    create(roleID): NodeEntry 


### NodeEntry

Records the details about all of a roles containers on a node. The
`active` field records the no. of containers currently active.

    active: int
    requested: transient int
    releasing: transient int
    last_used: long
    
    NodeEntry.available(): boolean = active-releasing == 0 and requested == 0

The two fields releasing and requested are used to track the ongoing
state of YARN requests: they do not need to be persisted across freeze/thaw
cycles. They may be relevant across AM restart, but without other data
structures in the AM, not enough to track what the AM was up to before
it was restarted. The strategy will be to ignore unexpected allocation
responses (which may come from pre-restart) requests, while treating
unexpected container release responses as failures.

The `active` counter is only updated after a container release response
has been received.

### Container Priority

The container priority field (a 32 bit integer) is currently used by Hoya
to index the specific role in a container so as to determine which role
has been offered in a container allocation message, and which role has
been released on a release event.

The RoleHistory appears to need to track outstanding requests, so that
when an allocation comes in, it can be mapped back to the request.
Simply looking up the nodes on the provided container and decrementing
its request counter is not going to work -the container may be allocated
on a different node from that requested.

**Proposal**: The priority field of a request is divided by Hoya into 8 bits for role ID,
24 bits for requestID. The request ID will be a simple
rolling integer -Hoya will assume that after 2^24 requests per role, it can be rolled,
-though as we will be retaining a list of outstanding requests, a clash should not occur.
The main requirement  is: not have > 2^24 outstanding requests for instances of a specific role,
which places an upper bound on the size of a Hoya cluster.

### OutstandingRequest ###

Tracks an outstanding request. This is used to correlate an allocation response
(whose Container Priority file is used to locate this request), with the
node and role used in the request.

      roleID:  int
      requestID :  int
      node: string
      requestedTime: long
      priority: int = requestID << 24 | roleID

### OutstandingRequestTracker ###

Contains a map from requestID to the specific `OutstandingRequest` made.

    lastID: int
    requestMap(RequestID) -> OutstandingRequest

Operations

    addRequest(Node, RoleID) -> OutstandingRequest 
        (and an updated request Map with a new entry)

### RecentlyReleasedList ###

An optimization: for each role retain a short list of nodes that were released.

1. When a role instance on that node is requested, it should be removed from
 this list (irrespective of how that node what chosen).
1. When a role instance is released *after an explicit release request*,
 if there are no role instances or outstanding
requests, its node Id should be pushed to the front
of the RecentlyReleasedList for that role.
1. When a node is needed for a new request, this list is consulted first.

This list is not persisted -when a Hoya Cluster is frozen it is moot, and when
an AM is restarted this optimisation can be built up as and when instances
are released.

The use of an explicit release request ensures that a failing node is
not placed at the head of this list.

**Update: this could simply be implemented as having a sorted list of
nodes for every role, using the last_used time as the sort order ** 

## Actions

### Bootstrap

1. Persistent Role History file not found; empty data structures created.

### Thaw

When thawing, the role history should be loaded -if it is missing Hoya
must revert to the bootstrap actions.

If found, the role history will contain Hoya's view of the Hoya Cluster's
state at the time the history was saved, explicitly recording the last-used
time of all nodes no longer hosting a role's container. By noting which roles
were actually being served, it implicitly nodes which nodes have a last_used
value greater than any of the last_used fields persisted in the file. That is:
all node entries listed as having active nodes at the time the history was
saved must have more recent data than those nodes listed as inactive.

When rebuilding the data structures, the fact that nodes were active at
save time must be converted into the data that indicates that the nodes
were at least in use *at the time the data was saved*. The state of the cluster
after the last save is unknown.

1. Role History loaded; Failure => Bootstrap.
1. Future: if role list enum != current enum, remapping could take place.
   Until then: fail.
1. iterate through all NodeEntry instances in each Node and set its last_used
time to the time the role history was last saved. 
1. Maybe: purge entries > N weeks older than the save time of the role history

If the list of recently used nodes is built by creating a sorted list of the
nodes for each role, this sort must take place after the previous operations
have completed. These sorted lists would have the active-at-time-of-save
nodes at their heads, followed by the inactive nodes in the order of the most
recently used.

### AM Restart

1. RoleHistory reloaded
1. Failure => create empty map.
1. Success => set the active counts for each NodeEntry to 0, with the `last_used`
value to that of the file's save time.
1. Enum existing containers and determine role of each container.
1. For each Node: create a NodeEntry each role with a container on that node, then
set the `NodeEntry.active` count to the #of containers on that node.
1. For each Node in the NodeMap, if there are no active instances of a role
 in that Node, then it could be added to list of available nodes for that
 instance.
1. For other (existing) container data structures in the AM, add entries
recording container details there.
1. Set the outstanding request queue to `null`, and reset the counters.
1. Trigger a cluster node count review -and request & release nodes as appropriate

**Issue**: what if requests come in for a (role, requestID) for
the previous instance of the AM? Could we just always set the initial
requestId counter to a random number and hope the collision rate is very, very 
low (2^24 * #(outstanding_requests)). If YARN-1041 ensures that
a restarted AM does not receive outstanding requests, this issue goes away.


### Teardown

1. If dirty, save role history to its file.
1. Issue release requests
1. Maybe update data structures on responses, but do not mark Role History
as dirty or flush it to disk.

This strategy is designed to eliminate the expectation that there will ever
be a clean shutdown -and so that the startup-time code should expect
the Role History to have been written during shutdown. Instead the code
should assume that the history was saved to disk at some point during the life
of the Hoya Cluster -ideally after the most recent change, and that the information
in it is only an approximate about what the previous state of the cluster was.

### Flex: Requesting a container in role `role`

    t: long = now() or if booting: t = rolehistory.saved 
    node = the node in NodeMap where node.nodeEntry[roleId].available()
        and diff(t, last_used) is considered 'recent'.
    if node!=null :
      N.nodeEntry[roleId].requested++;
      issue request with lax criteria
    else:
      set node==null in request
      
    outstanding = outstandingRequestTracker.addRequest(node, roleId)
    request.node = node
    request.priority = outstanding.priority
      
    //update existing Hoya role status
    roleStatus[roleId].incRequested();
      
      
There is a bias here towards previous nodes, even if the number of nodes
in the cluster has changed. This is why a node is picked where the number
of `active-releasing ==0 and requested == 0`, rather than where it is simply the lowest
value of `active+requested-releasing`: if there is no node in the nodemap that
is not running an instance of that role, it is left to the RM to decide where
the role instance should be instantiated.

This bias towards previously used nodes also means that (lax) requests
will be made of nodes that are currently unavailable either because they
are offline or simply overloaded with other work. In such circumstances,
the node will have an active count of zero -so the search will find these
nodes and request them -even though the requests cannot be satisfied.
As a result, the request will be downgraded to a rack-local or cluster-wide,
request -an acceptable degradation on a cluster where all the other entries
in the nodemap have instances of that specific node -but not when there are
empty nodes. 

#### Solutions

1. add some randomness in the search of the datastructure, rather than simply
iterate through the values. This would prevent the same unsatisfiable
node from being requested first.

1. keep track of requests, perhaps through a last-requested counter -and use
this in the selection process. This would radically complicate the selection
algorithm, and would not even distinguish "node recently released that was
also the last requested" from "node that has not recently satisifed requests
even though it was recently requested".
  
1. keep track of requests that weren't satisifed, so identify a node that
isn't currently satisfying requests.
     
    
#### History Issues 

Without using that history, there is a risk that a very old assignment
is used in place of a recent one and the value of locality decreased.

But there are consequences:
    
**Performance**:


Using the history to pick a recent node may increase selection times on a
large cluster, as for every instance needed, a scan of all nodes in the
nodemap is required (unless there is some clever bulk assignment list being built
up), or a sorted version of the nodemap is maintained, with a node placed
at the front of this list whenever its is updated.

**Thaw-time problems**

There is also the risk that while thawing, the `rolehistory.saved`
flag may be updated while the cluster flex is in progress, so making the saved
nodes appear out of date. Perhaps the list of recently released nodes could
be rebuilt at thaw time.


The proposed `recentlyReleasedList` addresses this, though it creates
another data structure to maintain and rebuild at cluster thaw time
from the last-used fields in the node entries.



### Flex: Removing a role instance from the cluster

Simple strategy:

    //find a node with at least one active container
    pick a node N in nodemap where for NodeEntry[roleID]: active > releasing; 
    nodeentry = node.get(roleID)
    nodeentry.active--;
    
Advanced Strategy:
    
    Scan through the map looking for a node where active >1 && active > releasing.
    If none are found, fall back to the previous strategy

This is guaranteed to release a container on any node with >1 container in use,
if such a node exists. If not, the scan time has increased to #(nodes).

Once a node has been identified

1. a container on it is located (via the existing
container map)
1. the 
1. the RM asked to release that container.
1. the (existing) containersBeingReleased Map has the container inserted into it




### AM Callback : Allocation 

    void onContainersAllocated(List<Container> allocatedContainers) 

This is the callback recevied when containers have been allocated.
Due to (apparently) race conditions, the AM may receive duplicate
container allocations -Hoya already has to recognise this and 
currently simply discards any surplus.


If the AM tracks outstanding requests made for specific hosts, it
will need to correlate allocations with requests, so as to decrement
the node-specific request count. Decrementing the request count
on the allocated node will not work, as the allocation may not be
to the node originally requested.

    C = container allocation
    roleID = C.priority & 0xff
    nodeID = C.nodeID
    outstanding = outstandingRequestTracker.remove(C.priority)
    if outstanding==null :
      raise UnknownRequest
    
    nodemap = rolemap.getNodeMap(role)
    node = nodemap.get(nodeID)
    if node == null :
      node = new Node()
      nodemap.put(nodeID, node)
    nodeentry = node.get(roleID)
    if nodeentry == null :
      nodeentry = new NodeEntry()
      node[roleID] = nodeentry
      nodeentry.active = 1
    else:
      if nodeentry.requested > 0:
        nodeentry.requested--
    nodeentry.active++
    nodemap.dirty = true
    
    //work back from request ID to node where the 
    //request was outstanding
    requestID = outstanding.nodeID
    if requestID != null:
      reqNode = nodeMap.get(requestID)
      reqNode.get(roleID).requested--;


    //update role summary data
    roleStatus[roleId].decRequested();
    roleStatus[roleId].incActive();
     
 
At end of this, there is a node in the nodemap, which has recorded that
there is now an active nodeentry for that role. The outstanding request has
been removed.

If a callback comes in for which there is no outstanding request, it is rejected
(logged, ignored, etc). This handles duplicate responses as well as any
other sync problem.
 
 
### AM callback onContainersCompleted: 

    void onContainersCompleted(List<ContainerStatus> completedContainers)
    
This callback returns a list of containers that have completed.

These need to be split into successful completion of a release request
and containers which have failed. 

This is currently done by tracking whch containers have been queued
for release (the AM contains a map `ContainerID->Container` to track this)


    if (getContainersBeingReleased().containsKey(containerID)):
      handle container completion
    else
      handle container failure
        


#### onContainersCompleted:Container Failure
    
A container is considered to have failed if the 
   
    nodeID = container.nodeID
    roleID = container.priority
    nodeentry = nodemap[nodeID]
    containerInfo = containermap(containerId)
    if containerInfo.released:
      containermap.delete(containerId)
      nodentry.releasing--;
    else:
    If the container has been marked as release-in-progress in the container map,
    release it.
    If the #of instances of the role < desired: 
    Re-request a single container on same node
 
Q. what if the node is oversubscribed?
A. Don't care: still try and restart the service there
 
 
#### Container Completed after being explicitly released

An explicit container request has responded confirming that the release
has completed
 
     roleID = C.priority & 0xff
     nodeID = C.nodeID
     node = nodemap.get(nodeID)
     if node != null :
       nodeentry = node.get(roleID)
     else
       nodeentry = null
     if nodeentry != null :
       nodeentry.releasing --
       nodentry.last_used = now()
       nodeentry.active++;

       nodemap.dirty = true
       if nodeentry.available():
         recentlyReleasedList.get(roleID).push((node, now())
     else:
       warn "release of container not in the nodemap]
     //update existing Hoya role status
     roleStatus[roleId].decReleased();
     containersBeingReleased.remove(containerID)

This algorithm handles the unexpected release of a container of which it
has no record of simply by warning of the event. It does not
add that entry to the nodemap