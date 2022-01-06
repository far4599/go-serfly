package types

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
