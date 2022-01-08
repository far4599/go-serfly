# go-serfly
Embed service discovery with raft-like leader election. Raft-like implementation does not contain log ang log replication logic and is used only for leader election.
This library let you create a cluster of different services, each service can have its own leader.

## Usage
Look at ```membership_test.go``` for examples of usage