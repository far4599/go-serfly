package go_serfly

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/hashicorp/serf/serf"
	jsoniter "github.com/json-iterator/go"
	"go.uber.org/zap"
)

var (
	RequestVoteTimeout     = 100 * time.Millisecond
	LeaderHeartbeatTimeout = 100 * time.Millisecond
	baseElectionTimeout    = 1000
	electionTimeoutMeasure = time.Millisecond
)

const (
	raftMessageRequestVote     = "requestVote"
	raftMessageLeaderHeartbeat = "leaderHeartbeat"
)

type RaftStatus int8

const (
	RaftStatusFollower = iota
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

type RequestVoteReq struct {
	Term        int
	CandidateID string
}

type RequestVoteResp struct {
	Term        int
	VoteGranted bool
	CandidateID string
}

type LeaderHeartbeatReq struct {
	Term     int
	LeaderID string
}

type LeaderHeartbeatResp struct {
	Term int
}

type raftConsensusModule struct {
	membership *Membership
	logger     *zap.Logger

	id                string
	currentTerm       int
	votedForID        string
	electionResetTime time.Time
	status            RaftStatus

	mu   sync.Mutex
	once sync.Once
}

func newRaft(name string, m *Membership, l *zap.Logger) *raftConsensusModule {
	r := &raftConsensusModule{
		id:          name,
		currentTerm: -1,
		votedForID:  "",
		status:      RaftStatusFollower,
		membership:  m,
		logger:      l,
	}

	m.AddMessageListener(raftMessageRequestVote, r.handleRequestVote)
	m.AddMessageListener(raftMessageLeaderHeartbeat, r.handleLeaderHeartbeat)
	m.AddMemberEventListener(memberEventBecameLeader, r.onBecameLeader)
	m.AddMemberEventListener(memberEventBecameFollower, r.onBecameFollower)

	go func() {
		// 3 seconds timeout to let serf complete cluster initialization
		time.Sleep(3 * time.Second)

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

	// This loops until either:
	// - we discover the election timer is no longer needed, or
	// - the election timer expires and this module becomes a candidate
	// In a follower, this typically keeps running in the background for the
	// duration of the module's lifetime.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

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

	checkElectionLoop()

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

	votesCount := 1

	peersCount := r.membership.ServiceMembers().CountActive()

	if peersCount == 1 {
		r.startLeader()
		return
	}

	respCh, err := r.membership.Broadcast(raftMessageRequestVote, RequestVoteReq{
		Term:        newTerm,
		CandidateID: r.id,
	}, &serf.QueryParam{
		RelayFactor: 2,
		Timeout:     RequestVoteTimeout,
	})
	if err != nil {
		return
	}

	for reply := range respCh {
		var resp RequestVoteResp
		err = jsoniter.Unmarshal(reply.Payload, &resp)
		if err != nil {
			return
		}

		if r.status != RaftStatusCandidate {
			return
		}

		if resp.Term > newTerm {
			fmt.Printf("elevtion dropped: collected %d votes, but term raised", votesCount)
			r.becomeFollower(resp.Term)
			return
		} else if resp.Term == newTerm {
			if resp.VoteGranted {
				votesCount += 1
				if votesCount*2 > peersCount+1 {
					// Won the election!
					r.startLeader()
					return
				}
			}
		}
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

		r.leaderSendHeartbeats()

		// Send periodic heartbeats, as long as still leader.
		for time.Now(); true; <-ticker.C {
			r.leaderSendHeartbeats()

			r.mu.Lock()
			if r.status != RaftStatusLeader {
				r.mu.Unlock()
				return
			}
			r.mu.Unlock()
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

	respCh, _ := r.membership.Broadcast(raftMessageLeaderHeartbeat, LeaderHeartbeatReq{
		Term:     currentTerm,
		LeaderID: r.id,
	}, &serf.QueryParam{
		RelayFactor: 0,
		Timeout:     LeaderHeartbeatTimeout,
	})

	checkReply := func(resp RequestVoteResp) bool {
		r.mu.Lock()
		defer r.mu.Unlock()

		if resp.Term > currentTerm {
			r.becomeFollower(resp.Term)
			return true
		}

		return false
	}

	for reply := range respCh {
		var resp RequestVoteResp
		err := jsoniter.Unmarshal(reply.Payload, &resp)
		if err != nil {
			continue
		}

		if checkReply(resp) {
			break
		}
	}
}

func (r *raftConsensusModule) handleLeaderHeartbeat(_ *Membership, query *serf.Query) {
	r.mu.Lock()

	if r.status == RaftStatusDead {
		r.mu.Unlock()
		return
	}

	var req LeaderHeartbeatReq
	err := jsoniter.Unmarshal(query.Payload, &req)
	if err != nil {
		r.mu.Unlock()
		return
	}

	if req.LeaderID == r.id {
		r.mu.Unlock()
		return
	}

	if req.Term > r.currentTerm {
		r.becomeFollower(req.Term)
	}

	resp := LeaderHeartbeatResp{
		Term: r.currentTerm,
	}

	if req.Term == r.currentTerm {
		if r.status != RaftStatusFollower {
			r.becomeFollower(req.Term)
		}
		r.electionResetTime = time.Now()
	}

	r.mu.Unlock()

	respJSON, err := jsoniter.Marshal(resp)
	if err != nil {
		return
	}

	err = query.Respond(respJSON)
	if err != nil {
		return
	}
}

func (r *raftConsensusModule) handleRequestVote(_ *Membership, query *serf.Query) {
	r.mu.Lock()

	if r.status == RaftStatusDead {
		r.mu.Unlock()
		return
	}

	var req RequestVoteReq
	err := jsoniter.Unmarshal(query.Payload, &req)
	if err != nil {
		r.mu.Unlock()
		return
	}

	if req.CandidateID == r.id {
		r.mu.Unlock()
		return
	}

	if req.Term > r.currentTerm {
		r.becomeFollower(req.Term)
	}

	resp := RequestVoteResp{
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

	r.mu.Unlock()

	respJSON, err := jsoniter.Marshal(resp)
	if err != nil {
		return
	}

	err = query.Respond(respJSON)
	if err != nil {
		return
	}
}

// electionTimeout generates a pseudo-random election timeout duration.
func (r *raftConsensusModule) electionTimeout() time.Duration {
	dynamicTimeout := r.membership.serf.DefaultQueryTimeout() / 6

	timeout := dynamicTimeout + time.Duration(rand.Intn(baseElectionTimeout))*electionTimeoutMeasure

	r.once.Do(func() {
		timeout = time.Duration(150+rand.Intn(baseElectionTimeout)) * electionTimeoutMeasure
	})

	return timeout
}

// Stop stops raft module
func (r *raftConsensusModule) Stop() {
	r.mu.Lock()
	r.status = RaftStatusDead
	r.mu.Unlock()
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
