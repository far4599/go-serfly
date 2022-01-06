package go_serfly

import "github.com/hashicorp/serf/serf"

const (
	LeaderTagName  = "raft_leader"
	ServiceTagName = "service"
)

type Members []serf.Member

func (mm Members) Count() int {
	return len(mm)
}

func (mm Members) CountActive() int {
	var count int

	for _, m := range mm {
		if m.Status == serf.StatusAlive {
			count++
		}
	}

	return count
}

func (mm Members) Active() Members {
	var members []serf.Member

	for _, m := range mm {
		if m.Status == serf.StatusAlive {
			members = append(members, m)
		}
	}

	return members
}

func (mm Members) GetService(serviceName string) Members {
	var members []serf.Member

	for _, member := range mm {
		if v, ok := member.Tags[ServiceTagName]; ok && v == serviceName {
			members = append(members, member)
		}
	}

	return members
}

func (mm Members) GetLeader() *serf.Member {
	for _, member := range mm {
		if _, ok := member.Tags[LeaderTagName]; ok {
			return &member
		}
	}

	return nil
}
