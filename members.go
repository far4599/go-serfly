package go_serfly

import "github.com/hashicorp/serf/serf"

const (
	LeaderTagName  = "raft_leader"
	ServiceTagName = "service"
)

// Members is a list of cluster members
type Members []serf.Member

// Count counts cluster members in the list
func (mm Members) Count() int {
	return len(mm)
}

// CountActive counts only active (healthy) cluster members
func (mm Members) CountActive() int {
	var count int

	for _, m := range mm {
		if m.Status == serf.StatusAlive {
			count++
		}
	}

	return count
}

// Active filters the list and returns a list of only active cluster members
func (mm Members) Active() Members {
	var members []serf.Member

	for _, m := range mm {
		if m.Status == serf.StatusAlive {
			members = append(members, m)
		}
	}

	return members
}

// GetService returns a list of cluster members of certain service
func (mm Members) GetService(serviceName string) Members {
	var members []serf.Member

	for _, member := range mm {
		if v, ok := member.Tags[ServiceTagName]; ok && v == serviceName {
			members = append(members, member)
		}
	}

	return members
}

// GetLeader returns the first member of the cluster with leader tag
func (mm Members) GetLeader() *serf.Member {
	for _, member := range mm {
		if _, ok := member.Tags[LeaderTagName]; ok {
			return &member
		}
	}

	return nil
}
