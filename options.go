package go_serfly

import (
	"github.com/far4599/go-serfly/types"
	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

type opt func(membership *Membership) error

// WithLogger sets logger for go-serfly
func WithLogger(logger *zap.Logger) func(membership *Membership) error {
	return func(membership *Membership) error {
		membership.logger = logger
		return nil
	}
}

// WithSerfLogger sets logger for serf package
func WithSerfLogger(logger *zap.Logger) func(membership *Membership) error {
	return func(membership *Membership) error {
		membership.serfLogger = zap.NewStdLog(logger)
		return nil
	}
}

// WithServiceName sets service name tag for the cluster member
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

// WithRaftTransport sets raft transport implementation
func WithRaftTransport(t RaftTransport) func(membership *Membership) error {
	return func(membership *Membership) error {
		membership.raftTransport = t
		membership.Config.Tags = t.AddTransportTags(membership.Config.Tags)
		return nil
	}
}

// WithOnMemberJoinCallback adds callback to handle member join event
func WithOnMemberJoinCallback(fn func(*Membership, *serf.Member)) func(membership *Membership) error {
	return func(membership *Membership) error {
		membership.AddMemberEventListener(types.MemberEventJoin, fn)
		return nil
	}
}

// WithOnMemberLeaveCallback adds callback to handle member leave event
func WithOnMemberLeaveCallback(fn func(*Membership, *serf.Member)) func(membership *Membership) error {
	return func(membership *Membership) error {
		membership.AddMemberEventListener(types.MemberEventLeave, fn)
		return nil
	}
}

// WithOnBecomeLeaderCallback adds callback to handle an event of member becomes a leader
func WithOnBecomeLeaderCallback(fn func(*Membership, *serf.Member)) func(membership *Membership) error {
	return func(membership *Membership) error {
		membership.AddMemberEventListener(types.MemberEventBecameLeader, fn)
		return nil
	}
}

// WithOnBecomeFollowerCallback adds callback to handle an event of member becomes a follower
func WithOnBecomeFollowerCallback(fn func(*Membership, *serf.Member)) func(membership *Membership) error {
	return func(membership *Membership) error {
		membership.AddMemberEventListener(types.MemberEventBecameFollower, fn)
		return nil
	}
}

// WithOnBecomeCandidateCallback adds callback to handle an event of member becomes a candidate
func WithOnBecomeCandidateCallback(fn func(*Membership, *serf.Member)) func(membership *Membership) error {
	return func(membership *Membership) error {
		membership.AddMemberEventListener(types.MemberEventBecameCandidate, fn)
		return nil
	}
}
