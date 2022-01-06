package go_serfly

import (
	"context"

	"github.com/far4599/go-serfly/types"
	"github.com/hashicorp/serf/serf"
)

type RaftTransport interface {
	SendVoteRequest(ctx context.Context, target serf.Member, msg types.RequestVoteReq) (*types.RequestVoteResp, error)
	SendLeaderHeartbeat(ctx context.Context, target serf.Member, msg types.LeaderHeartbeatReq) (*types.LeaderHeartbeatResp, error)
	SetRequestVoteHandler(func(ctx context.Context, req types.RequestVoteReq) (*types.RequestVoteResp, error)) error
	SetLeaderHeartbeatHandler(func(ctx context.Context, req types.LeaderHeartbeatReq) (*types.LeaderHeartbeatResp, error)) error
	AddTransportTags(tags map[string]string) map[string]string
	Start() error
	Stop() error
}
