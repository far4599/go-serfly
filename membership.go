package go_serfly

import (
	"log"
	"net"
	"time"

	"github.com/far4599/go-serfly/types"
	"github.com/google/uuid"
	"github.com/hashicorp/serf/serf"
	jsoniter "github.com/json-iterator/go"
	"go.uber.org/zap"
)

type Config struct {
	Name                  string            // cluster member name
	Addr                  string            // member tcp address ("ip:port")
	Tags                  map[string]string // member tags
	KnownClusterAddresses []string          // initial known addresses of cluster members
}

type Membership struct {
	Config

	serf          *serf.Serf
	raft          *raftConsensusModule
	raftTransport RaftTransport
	logger        *zap.Logger
	serfLogger    *log.Logger

	serviceName string

	eventsCh             chan serf.Event
	messageListeners     map[string][]func(*Membership, *serf.Query)
	memberEventListeners map[types.MemberEventType][]func(*Membership, *serf.Member)
}

// NewMembership is go-serfly membership constructor
func NewMembership(config Config, opts ...opt) (*Membership, error) {
	c := &Membership{
		Config:     config,
		logger:     zap.NewNop(),
		serfLogger: zap.NewStdLog(zap.NewNop()),

		messageListeners:     make(map[string][]func(*Membership, *serf.Query)),
		memberEventListeners: make(map[types.MemberEventType][]func(*Membership, *serf.Member)),
	}

	for _, optFn := range opts {
		if err := optFn(c); err != nil {
			return nil, err
		}
	}

	return c, nil
}

// Serve setups a new cluster member and joins the cluster
func (m *Membership) Serve() error {
	addr, err := net.ResolveTCPAddr("tcp", m.Addr)
	if err != nil {
		return err
	}

	config := serf.DefaultConfig()
	config.Init()

	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port

	config.MemberlistConfig.Logger = m.serfLogger
	config.Logger = m.serfLogger

	m.eventsCh = make(chan serf.Event)
	config.EventCh = m.eventsCh
	config.Tags = m.Tags

	// generate a unique name for the node if not specified
	if m.Config.Name == "" {
		name := uuid.New().String()
		m.Config.Name = name
	}
	config.NodeName = m.Config.Name

	m.serf, err = serf.Create(config)
	if err != nil {
		return err
	}

	go m.eventHandler()

	m.raft = newRaft(m.serf.LocalMember().Name, m, m.raftTransport, m.logger)

	if m.KnownClusterAddresses != nil {
		_, err = m.serf.Join(m.KnownClusterAddresses, true)
		if err != nil {
			return err
		}
	}

	return nil
}

// eventHandler handles serf events
func (m *Membership) eventHandler() {
	for e := range m.eventsCh {
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member.Name) {
					continue
				}

				m.onMemberEventHook(types.MemberEventJoin, &member)
			}
		case serf.EventMemberLeave, serf.EventMemberFailed:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member.Name) {
					return
				}

				m.onMemberEventHook(types.MemberEventLeave, &member)
			}
		case serf.EventQuery:
			query, ok := e.(*serf.Query)
			if !ok {
				return
			}

			m.onMessageHook(query.Name, query)
		}
	}
}

// Broadcast sends query to members, you may specify targets by using filters in params
func (m *Membership) Broadcast(msgType string, msgPayload interface{}, params *serf.QueryParam) (<-chan types.BroadcastMessageResp, error) {
	msgJSON, err := jsoniter.Marshal(msgPayload)
	if err != nil {
		return nil, err
	}

	if params == nil {
		params = &serf.QueryParam{
			RelayFactor: 2,
			Timeout:     100 * time.Millisecond,
		}
	}

	if params.FilterTags == nil && m.serviceName != "" {
		params.FilterTags = map[string]string{ServiceTagName: m.serviceName}
	}

	resp, err := m.serf.Query(msgType, msgJSON, params)
	if err != nil {
		return nil, err
	}

	respCh := make(chan types.BroadcastMessageResp, cap(resp.ResponseCh()))

	go func() {
		defer close(respCh)

		for r := range resp.ResponseCh() {
			respCh <- types.BroadcastMessageResp{
				RequestName: msgType,
				FromID:      r.From,
				Payload:     r.Payload,
			}
		}
	}()

	return respCh, nil
}

// isLocal checks if node name equals to the local node name
func (m *Membership) isLocal(name string) bool {
	return m.serf.LocalMember().Name == name
}

// IsLeader checks if the local node is a raft leader
func (m *Membership) IsLeader() bool {
	return m.raft != nil && m.raft.status == types.RaftStatusLeader
}

// Status returns raft status
func (m *Membership) Status() types.RaftStatus {
	return m.raft.status
}

// AllMembers returns list of all cluster members (including left or failed)
func (m *Membership) AllMembers() Members {
	return m.serf.Members()
}

// ServiceMembers returns list of cluster members with the same service name as local node
func (m *Membership) ServiceMembers() Members {
	var mm Members = m.serf.Members()

	if m.serviceName == "" {
		return mm
	}

	return mm.GetService(m.serviceName)
}

// Stop stops the membership service. Will result the local node to leave the cluster
func (m *Membership) Stop() error {
	if m.raft != nil {
		m.raft.Stop()
	}

	err := m.serf.Leave()
	if err != nil {
		return err
	}

	return nil
}
