package go_serfly

import (
	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

func WithLogger(logger *zap.Logger) func(membership *Membership) error {
	return func(membership *Membership) error {
		membership.logger = logger
		return nil
	}
}

func WithServiceName(serviceName string) func(membership *Membership) error {
	return func(membership *Membership) error {
		membership.serviceName = serviceName
		if membership.Config.Tags == nil {
			membership.Config.Tags = make(map[string]string)
		}
		membership.Config.Tags[ServiceTagName] = serviceName
		return nil
	}
}

func WithRaftTransport(t RaftTransport) func(membership *Membership) error {
	return func(membership *Membership) error {
		membership.raftTransport = t
		membership.Config.Tags = t.AddTransportTags(membership.Config.Tags)
		return nil
	}
}

func WithOnMemberJoinCallback(fn func(*Membership, *serf.Member)) func(membership *Membership) error {
	return func(membership *Membership) error {
		membership.AddMemberEventListener(memberEventJoin, fn)
		return nil
	}
}

func WithOnMemberLeaveCallback(fn func(*Membership, *serf.Member)) func(membership *Membership) error {
	return func(membership *Membership) error {
		membership.AddMemberEventListener(memberEventLeave, fn)
		return nil
	}
}

func WithOnBecomeLeaderCallback(fn func(*Membership, *serf.Member)) func(membership *Membership) error {
	return func(membership *Membership) error {
		membership.AddMemberEventListener(memberEventBecameLeader, fn)
		return nil
	}
}

func WithOnBecomeFollowerCallback(fn func(*Membership, *serf.Member)) func(membership *Membership) error {
	return func(membership *Membership) error {
		membership.AddMemberEventListener(memberEventBecameFollower, fn)
		return nil
	}
}

func WithOnBecomeCandidateCallback(fn func(*Membership, *serf.Member)) func(membership *Membership) error {
	return func(membership *Membership) error {
		membership.AddMemberEventListener(memberEventBecameCandidate, fn)
		return nil
	}
}
