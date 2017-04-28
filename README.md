Valkyrie is a system to schedule docker containers running on cloud computing.
Docker hosts are Valkyrie slaves and communicate through SQS queues to discover
what Valkyrie recipe they should use: which docker containers to run and how
many of them should be active at the same time.  The Valkyrie master is
responsible for communicating with the Valkyrie slaves, calculate recipes and
distributing them among the Valkyrie slaves. The Valkyrie master also tracks
the costs endured for hosting the slaves and tries to minimize the costs.  It
will start new instances of Valkyrie slaves and shut them down when the
workload is too low.


TODO:
- describe the protocol between slave and master
- improve logging
- should running docker instances be killed after a shutdown signal from the master? maybe a kill=True option?
