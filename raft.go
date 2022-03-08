// This is a raft-like consensus module implementation. It is based on raft.
// Actually it is used for leader election only. There is no log and log
// replication at all. So all the nodes in the cluster has the same chances to
// become a leader.

package go_serfly

import (
	"context"
	"sync"
	"time"

	"github.com/far4599/go-serfly/types"
	"github.com/hashicorp/serf/serf"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	electionTimeout               = 150
	runElectionTimerInterval      = 20 * time.Millisecond
	leaderHeartbeatsTimerInterval = 10 * time.Millisecond
)

type raftConsensusModule struct {
	membership *Membership
	transport  RaftTransport

	id                string    // current cluster node name
	term              int       // current raft term
	votedForID        string    // id of the member this node voted for
	electionResetTime time.Time // the last time we reset election timer
	status            types.RaftStatus

	singleNodeCanBecomeLeader bool

	logger *zap.Logger
	mutex  sync.Mutex
}

// newRaft is a constructor of a new raft consensus module
func newRaft(name string, m *Membership, t RaftTransport, l *zap.Logger, singleNodeCanBecomeLeader bool) *raftConsensusModule {
	if l == nil {
		l = zap.NewNop()
	}

	if t == nil {
		l.Fatal("raft transport implementation is not provided")
	}

	r := &raftConsensusModule{
		id:                        name,
		term:                      -1,
		votedForID:                "",
		status:                    types.RaftStatusFollower,
		membership:                m,
		transport:                 t,
		singleNodeCanBecomeLeader: singleNodeCanBecomeLeader,
		logger:                    l,
	}

	// custom handler for local member events
	m.AddMemberEventListener(types.MemberEventBecameLeader, r.onBecameLeader)
	m.AddMemberEventListener(types.MemberEventBecameFollower, r.onBecameFollower)

	if err := t.SetLeaderHeartbeatHandler(r.handleLeaderHeartbeat); err != nil {
		l.Fatal("failed to LeaderHeartbeatHandler", zap.Error(err))
	}

	if err := t.SetRequestVoteHandler(r.handleRequestVote); err != nil {
		l.Fatal("failed to SetRequestVoteHandler", zap.Error(err))
	}

	go func() {
		if err := t.Start(); err != nil {
			l.Fatal("raft transport failed to start", zap.Error(err))
		}
	}()

	go func() {
		r.mutex.Lock()
		r.electionResetTime = time.Now()
		r.mutex.Unlock()
		r.runElectionTimer()
	}()

	return r
}

// runElectionTimer implements an election timer. it should be launched whenever
// we want to start a timer towards becoming a candidate in a new election.
func (r *raftConsensusModule) runElectionTimer() {
	timeoutDuration := r.electionTimeout()

	r.mutex.Lock()
	initialTerm := r.term
	r.mutex.Unlock()

	checkElectionLoop := func() (exitLoop bool) {
		r.mutex.Lock()
		defer r.mutex.Unlock()

		if r.status != types.RaftStatusCandidate && r.status != types.RaftStatusFollower {
			return true
		}

		if initialTerm != r.term {
			return true
		}

		// start a new election if we haven't heard heartbeats from a leader or
		// haven't voted for someone for the duration of the timeout
		if elapsed := time.Since(r.electionResetTime); elapsed >= timeoutDuration {
			r.startElection()
			return true
		}

		return false
	}

	ticker := time.NewTicker(runElectionTimerInterval)
	defer ticker.Stop()

	for time.Now(); true; <-ticker.C {
		if checkElectionLoop() {
			break
		}
	}
}

// startElection starts a new election with this member as a candidate.
// expects mutex to be locked.
func (r *raftConsensusModule) startElection() {
	defer func() {
		go r.runElectionTimer()
	}()

	peers := r.membership.ServiceMembers().Active()

	// if member is alone, it cannot become a leader
	if len(peers) <= 1 && !r.singleNodeCanBecomeLeader {
		return
	}

	r.status = types.RaftStatusCandidate
	r.votedForID = r.id
	newTerm := r.term + 1
	r.term = newTerm
	r.electionResetTime = time.Now()

	go r.membership.onMemberEventHook(types.MemberEventBecameCandidate, nil)

	// if node is single in the cluster, it becomes leader immediately
	if len(peers) == 1 && r.singleNodeCanBecomeLeader {
		r.startLeader()
		return
	}

	req := types.RequestVoteReq{
		Term:        newTerm,
		CandidateID: r.id,
	}

	votesCount := atomic.NewUint32(1)
	for _, peer := range peers {
		if peer.Name == r.id {
			continue
		}

		go func(peer serf.Member) {
			ctx, cancelCtx := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancelCtx()

			resp, err := r.transport.SendVoteRequest(ctx, peer, req)
			if err != nil {
				return
			}

			if r.status != types.RaftStatusCandidate {
				return
			}

			if resp.Term > newTerm {
				r.logger.Debug("resp term is greater than new term", zap.Int("respTerm", resp.Term), zap.Int("newTerm", newTerm))
				r.becomeFollower(resp.Term)
				return
			} else if resp.Term == newTerm {
				if resp.VoteGranted {
					votesCount.Add(1)
					if int(votesCount.Load())*2 > len(peers)+1 {
						// Won the election!
						r.startLeader()
						return
					}
				}
			}
		}(peer)
	}
}

// startLeader switches module into a leader state and begins process of heartbeats.
// expects mutex to be locked.
func (r *raftConsensusModule) startLeader() {
	r.status = types.RaftStatusLeader

	go r.membership.onMemberEventHook(types.MemberEventBecameLeader, nil)

	go func() {
		ticker := time.NewTicker(leaderHeartbeatsTimerInterval)
		defer ticker.Stop()

		// send periodic heartbeats as long as member is a leader
		for time.Now(); true; <-ticker.C {
			r.mutex.Lock()
			if r.status != types.RaftStatusLeader {
				r.mutex.Unlock()
				return
			}
			r.mutex.Unlock()

			r.leaderSendHeartbeats()
		}
	}()
}

// becomeFollower switches module into a follower state and resets module state.
// expects mutex to be locked.
func (r *raftConsensusModule) becomeFollower(term int) {
	r.status = types.RaftStatusFollower
	r.term = term
	r.votedForID = ""
	r.electionResetTime = time.Now()

	go r.membership.onMemberEventHook(types.MemberEventBecameFollower, nil)

	go r.runElectionTimer()
}

// leaderSendHeartbeats sends heartbeats to all service members in the cluster
func (r *raftConsensusModule) leaderSendHeartbeats() {
	r.mutex.Lock()
	currentTerm := r.term
	r.mutex.Unlock()

	peers := r.membership.ServiceMembers().Active()

	if len(peers) <= 1 {
		return
	}

	req := types.LeaderHeartbeatReq{
		Term:     currentTerm,
		LeaderID: r.id,
	}

	for _, peer := range peers {
		peer := peer

		if peer.Name == r.id {
			continue
		}

		go func() {

			ctx, cancelCtx := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancelCtx()

			resp, err := r.transport.SendLeaderHeartbeat(ctx, peer, req)
			if err != nil {
				return
			}

			r.mutex.Lock()
			defer r.mutex.Unlock()

			if resp.Term > currentTerm {
				r.logger.Debug("resp term is greater than term", zap.Int("respTerm", resp.Term), zap.Int("term", currentTerm))
				cancelCtx()
				r.becomeFollower(resp.Term)
			}
		}()
	}
}

// handleLeaderHeartbeat is a handler for RaftTransport.SetLeaderHeartbeatHandler
func (r *raftConsensusModule) handleLeaderHeartbeat(_ context.Context, req types.LeaderHeartbeatReq) (*types.LeaderHeartbeatResp, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.status == types.RaftStatusDead {
		return nil, nil
	}

	if req.Term > r.term {
		r.logger.Debug("new term in leader heartbeat", zap.Int("newTerm", req.Term), zap.Int("term", r.term))
		r.becomeFollower(req.Term)
	}

	resp := &types.LeaderHeartbeatResp{
		Term: r.term,
	}

	if req.Term == r.term {
		if r.status != types.RaftStatusFollower {
			r.logger.Debug("new term in leader heartbeat", zap.Int("newTerm", req.Term), zap.Int("term", r.term), zap.String("id", r.id), zap.String("leaderID", req.LeaderID))
			r.becomeFollower(req.Term)
		}
		r.electionResetTime = time.Now()
	}

	return resp, nil
}

// handleRequestVote is a handler for RaftTransport.SetRequestVoteHandler
func (r *raftConsensusModule) handleRequestVote(_ context.Context, req types.RequestVoteReq) (*types.RequestVoteResp, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.status == types.RaftStatusDead {
		return nil, nil
	}

	if req.Term > r.term {
		r.logger.Debug("req term is greater than current term", zap.Int("reqTerm", req.Term), zap.Int("term", r.term))
		r.becomeFollower(req.Term)
	}

	resp := &types.RequestVoteResp{
		Term: r.term,
	}

	if r.term == req.Term &&
		(r.votedForID == "" || r.votedForID == req.CandidateID) {
		resp.VoteGranted = true
		r.votedForID = req.CandidateID
		r.electionResetTime = time.Now()
	} else {
		resp.VoteGranted = false
	}

	return resp, nil
}

// Stop stops raft module
func (r *raftConsensusModule) Stop() {
	r.logger.Info("stopping raft module")

	r.mutex.Lock()
	r.status = types.RaftStatusDead
	r.mutex.Unlock()

	if err := r.transport.Stop(); err != nil {
		r.logger.Error("failed to stop raft transport", zap.Error(err))
	}
}

// onBecameLeader adds leader tag to the member tags list
func (r *raftConsensusModule) onBecameLeader(m *Membership, member *serf.Member) {
	tags := member.Tags

	if tags == nil {
		tags = make(map[string]string)
	}

	tags[types.LeaderTagName] = "true"

	m.Config.Tags = tags
	err := m.serf.SetTags(tags)
	if err != nil && r.status != types.RaftStatusDead {
		r.logger.Error("failed to add leader tag to the member tags", zap.Error(err), zap.String("memberName", member.Name))
	}
}

// onBecameLeader removes leader tag from the member tags list
func (r *raftConsensusModule) onBecameFollower(m *Membership, member *serf.Member) {
	tags := member.Tags

	if tags == nil {
		return
	} else if _, ok := tags[types.LeaderTagName]; !ok {
		return
	}

	delete(tags, types.LeaderTagName)

	m.Config.Tags = tags
	err := m.serf.SetTags(tags)
	if err != nil {
		r.logger.Error("failed to remove leader tag from the member tags", zap.Error(err), zap.String("memberName", member.Name))
	}
}

// electionTimeout generates a random election timeout duration in range [150,300] milliseconds
func (r *raftConsensusModule) electionTimeout() time.Duration {
	return (electionTimeout + time.Duration(randInt64n(int64(electionTimeout)))) * time.Millisecond
}
