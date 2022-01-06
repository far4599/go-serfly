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

type RaftStatus int8

const (
	RaftStatusFollower RaftStatus = iota
	RaftStatusCandidate
	RaftStatusLeader
	RaftStatusDead
)

func (s RaftStatus) String() string {
	switch s {
	case RaftStatusFollower:
		return "Follower"
	case RaftStatusCandidate:
		return "Candidate"
	case RaftStatusLeader:
		return "Leader"
	case RaftStatusDead:
		return "Dead"
	default:
		panic("unreachable")
	}
}

type raftConsensusModule struct {
	membership *Membership
	logger     *zap.Logger

	transport RaftTransport

	id                string
	currentTerm       int
	votedForID        string
	electionResetTime time.Time
	status            RaftStatus

	mu   sync.Mutex
	once sync.Once
}

func newRaft(name string, m *Membership, t RaftTransport, l *zap.Logger) *raftConsensusModule {
	if l == nil {
		l = zap.NewNop()
	}

	if t == nil {
		l.Fatal("raft transport is not set")
	}

	r := &raftConsensusModule{
		id:          name,
		currentTerm: -1,
		votedForID:  "",
		status:      RaftStatusFollower,
		membership:  m,

		transport: t,

		logger: l,
	}

	m.AddMemberEventListener(memberEventBecameLeader, r.onBecameLeader)
	m.AddMemberEventListener(memberEventBecameFollower, r.onBecameFollower)

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
		r.mu.Lock()
		r.electionResetTime = time.Now()
		r.mu.Unlock()
		r.runElectionTimer()
	}()

	return r
}

// runElectionTimer implements an election timer. It should be launched whenever
// we want to start a timer towards becoming a candidate in a new election.
//
// This function is blocking and should be launched in a separate goroutine;
// it's designed to work for a single (one-shot) election timer, as it exits
// whenever the module state changes from follower/candidate or the term changes.
func (r *raftConsensusModule) runElectionTimer() {
	timeoutDuration := r.electionTimeout()

	r.mu.Lock()
	initialTerm := r.currentTerm
	r.mu.Unlock()

	checkElectionLoop := func() (exitLoop bool) {
		r.mu.Lock()
		defer r.mu.Unlock()

		if r.status != RaftStatusCandidate && r.status != RaftStatusFollower {
			return true
		}

		if initialTerm != r.currentTerm {
			return true
		}

		// Start an election if we haven't heard from a leader or haven't voted for
		// someone for the duration of the timeout.
		if elapsed := time.Since(r.electionResetTime); elapsed >= timeoutDuration {
			r.startElection()
			return true
		}

		return false
	}

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for time.Now(); true; <-ticker.C {
		if checkElectionLoop() {
			break
		}
	}
}

// startElection starts a new election with this module as a candidate.
// Expects module.mu to be locked.
func (r *raftConsensusModule) startElection() {
	defer func() {
		go r.runElectionTimer()
	}()

	r.status = RaftStatusCandidate
	r.votedForID = r.id
	newTerm := r.currentTerm + 1
	r.currentTerm = newTerm
	r.electionResetTime = time.Now()

	go r.membership.onMemberEventHook(memberEventBecameCandidate, nil)

	peers := r.membership.ServiceMembers().Active()

	// if member is alone, it cannot become a leader
	if len(peers) == 1 {
		return
	}

	req := types.RequestVoteReq{
		Term:        newTerm,
		CandidateID: r.id,
	}

	var votesCount atomic.Uint32
	votesCount.Store(1)
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

			if r.status != RaftStatusCandidate {
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
// Expects module.mu to be locked.
func (r *raftConsensusModule) startLeader() {
	r.status = RaftStatusLeader

	go r.membership.onMemberEventHook(memberEventBecameLeader, nil)

	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()

		// Send periodic heartbeats, as long as still leader.
		for time.Now(); true; <-ticker.C {
			r.mu.Lock()
			if r.status != RaftStatusLeader {
				r.mu.Unlock()
				return
			}
			r.mu.Unlock()

			r.leaderSendHeartbeats()
		}
	}()
}

// becomeFollower makes module a follower and resets its state.
// Expects module.mu to be locked.
func (r *raftConsensusModule) becomeFollower(term int) {
	r.status = RaftStatusFollower
	r.currentTerm = term
	r.votedForID = ""
	r.electionResetTime = time.Now()

	go r.membership.onMemberEventHook(memberEventBecameFollower, nil)

	go r.runElectionTimer()
}

// leaderSendHeartbeats sends a round of heartbeats to all peerIDs, collects their
// replies and adjusts module's state.
func (r *raftConsensusModule) leaderSendHeartbeats() {
	r.mu.Lock()
	currentTerm := r.currentTerm
	r.mu.Unlock()

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

			r.mu.Lock()
			defer r.mu.Unlock()

			if resp.Term > currentTerm {
				r.logger.Debug("resp term is greater than currentTerm", zap.Int("respTerm", resp.Term), zap.Int("currentTerm", currentTerm))
				cancelCtx()
				r.becomeFollower(resp.Term)
			}
		}()
	}
}

func (r *raftConsensusModule) handleLeaderHeartbeat(_ context.Context, req types.LeaderHeartbeatReq) (*types.LeaderHeartbeatResp, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.status == RaftStatusDead {
		return nil, nil
	}

	if req.Term > r.currentTerm {
		r.logger.Debug("new term in leader heartbeat", zap.Int("newTerm", req.Term), zap.Int("currentTerm", r.currentTerm))
		r.becomeFollower(req.Term)
	}

	resp := &types.LeaderHeartbeatResp{
		Term: r.currentTerm,
	}

	if req.Term == r.currentTerm {
		if r.status != RaftStatusFollower {
			r.logger.Debug("new term in leader heartbeat", zap.Int("newTerm", req.Term), zap.Int("currentTerm", r.currentTerm), zap.String("id", r.id), zap.String("leaderID", req.LeaderID))
			r.becomeFollower(req.Term)
		}
		r.electionResetTime = time.Now()
	}

	return resp, nil
}

func (r *raftConsensusModule) handleRequestVote(_ context.Context, req types.RequestVoteReq) (*types.RequestVoteResp, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.status == RaftStatusDead {
		return nil, nil
	}

	if req.Term > r.currentTerm {
		r.logger.Debug("req term is greater than current term", zap.Int("reqTerm", req.Term), zap.Int("currentTerm", r.currentTerm))
		r.becomeFollower(req.Term)
	}

	resp := &types.RequestVoteResp{
		Term: r.currentTerm,
	}

	if r.currentTerm == req.Term &&
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
	r.mu.Lock()
	r.status = RaftStatusDead
	r.mu.Unlock()

	if err := r.transport.Stop(); err != nil {
		r.logger.Error("failed to stop raft transport", zap.Error(err))
	}
}

func (r *raftConsensusModule) onBecameLeader(m *Membership, member *serf.Member) {
	tags := member.Tags

	if tags == nil {
		tags = make(map[string]string)
	}

	tags[LeaderTagName] = "true"

	err := m.serf.SetTags(tags)
	if err != nil {
		// log error
	}
}

func (r *raftConsensusModule) onBecameFollower(m *Membership, member *serf.Member) {
	tags := member.Tags

	if tags == nil {
		return
	}

	delete(tags, LeaderTagName)

	err := m.serf.SetTags(tags)
	if err != nil {
		// log error
	}
}

// electionTimeout generates a random election timeout duration.
func (r *raftConsensusModule) electionTimeout() time.Duration {
	return time.Duration(150+randInt64n(int64(150))) * time.Millisecond
}
