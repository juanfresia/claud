# Introduction

As learned on previous courses, distributed systems present a lot of advantages
over monolithic services. Such advantages come over with an increase of the
system complexity, as the same is distributed between several nodes. Many
algorithms exist to face these situations, but we consider there are not many
practical examples of them.

Hence, looking forward to test some of these algorithms by our own, we proposed
to develop a distributed platform for orchestrating applications via a computers
cluster. This way, we had to deal with topics such as leader election, fault
tolerance and state reconciliation. The following sections further explain how
our cluster work, what problems we had to face and how we solved them.

# Architecture overview

CLAUD has a master-slave architecture, and nodes are therefore classified in two
categories: master nodes, and slave nodes. Master nodes have the responsibility
of keeping track of everything: which nodes are alive, which jobs exist and
where are they running. They also expose an API for users to interact with the
cluster. 

Master nodes go through a process for electing a leader node among themselves.
The leader node is the only one that can alter the state of the cluster, and all
write queries are forwarded to it. The other master slaves are considered
followers and act only as replicas of the state; whenever the leader fails, all
follower masters perform a new election and reconcile the state of the cluster.
See section on leader election for details on how this is performed, and how
reconciliation occurs.

On the other hand, slaves nodes are only responsible for running the jobs and 
keeping track of them. Slave nodes are then the worker nodes of the cluster,
being the ones yielding the resources. The following picture sumarizes this
whole architecture:

![](img/architecture.jpg)

# Masters leader election

The masters leader election is based on a very simple algorithm, in which the master
with the lowest ID is considered as the leader of the cluster. To successfully perform
that election, a discovery protocol must take part and all nodes have to announce
themselves via an UDP broadcast address. Then, after a certain amount of time, all
masters know which nodes are participating on the leader election, and can pick the
proper leader based on their IDs. The figure belows represent this situation:

![](img/leader_election.jpg)

# Nodes state reconciliation

Two options where considered when discussing which kind of information should
the master nodes maintain: 1) every master has a global view of the state of the
cluster, and 2) every master knows only a subset of tasks and slave nodes.

Option 2 seemed to be a more natural option for a distributed system, as
information is never centralized, and each master would be able to operate
independently of the other ones. However, it has the main disadvantage that it
makes the task of keeping track of available resources, and thus scheduling a
new job really hard.

Option 1 is really well suited for that, since any master can see at a glance
which resources are available because all of them store the global state. The
trick here is how to maintain that state consistent upon failure of any of the
members of the cluster. We took some time considering such edge cases and arrived 
to the conclusion that it would be manageable with proper state reconciliation (see
bellow).

Following this reasoning, we opted for option 1. Consequently, each master node
contains all the information of the state of the cluster. This information is
kept in two in-memory tables:

  - **Alive nodes**: lists every node, master or slave, in the cluster. Keeps
    track of resources of each node.
  - **Job list**: lists all the jobs in the cluster, its state and which node
    is currently running in.

# Job lifecycle
// Explain the different states of the job, and how transition occurs

//# Problems identified solved

# Conclusions

To sum up, we were able to successfully develop from scratch the proposed
platform, facing and solving many problems inherent to distributed systems. The
implemented cluster can launch jobs and reschedule them when a node fails, thus
providing its main service as an orchestration platform. Although we know there
are many things to improve, we consider the main goal for this project was
achieved, since we have successfully discussed and implemented various relevant
distributed algorithms.
