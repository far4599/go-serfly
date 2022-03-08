package types

type BroadcastMessageResp struct {
	RequestName string
	FromID      string
	Payload     []byte
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

type MemberEventType int

const (
	MemberEventJoin MemberEventType = iota
	MemberEventLeave
	MemberEventBecameFollower
	MemberEventBecameCandidate
	MemberEventBecameLeader
)
