package go_serfly

import (
	"net"
	"time"

	"github.com/hashicorp/serf/serf"
	jsoniter "github.com/json-iterator/go"
	"go.uber.org/zap"
)

type opt func(membership *Membership) error

type Config struct {
	NodeName              string
	BindAddr              string
	Tags                  map[string]string
	KnownClusterAddresses []string
}

type BroadcastMessageResp struct {
	RequestName string
	FromID      string
	Payload     []byte
}

type Membership struct {
	Config

	serf   *serf.Serf
	raft   *raftConsensusModule
	logger *zap.Logger

	serviceName string

	eventsCh             chan serf.Event
	messageListeners     map[string][]func(*Membership, *serf.Query)
	memberEventListeners map[memberEventType][]func(*Membership, *serf.Member)
}

func New(config Config, opts ...opt) (*Membership, error) {
	c := &Membership{
		Config: config,
		logger: zap.NewNop(),

		messageListeners:     make(map[string][]func(*Membership, *serf.Query)),
		memberEventListeners: make(map[memberEventType][]func(*Membership, *serf.Member)),
	}

	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}

	return c, nil
}

func (m *Membership) Serve() (err error) {
	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)
	if err != nil {
		return err
	}

	config := serf.DefaultConfig()
	config.Init()
	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port

	config.MemberlistConfig.Logger = zap.NewStdLog(m.logger)
	config.Logger = zap.NewStdLog(m.logger)

	m.eventsCh = make(chan serf.Event)
	config.EventCh = m.eventsCh
	config.Tags = m.Tags
	config.NodeName = m.Config.NodeName

	m.serf, err = serf.Create(config)
	if err != nil {
		return err
	}

	m.raft = newRaft(m.serf.LocalMember().Name, m, m.logger)

	go m.eventHandler()

	if m.KnownClusterAddresses != nil {
		_, err = m.serf.Join(m.KnownClusterAddresses, true)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Membership) eventHandler() {
	for e := range m.eventsCh {
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member.Name) {
					continue
				}

				m.onMemberEventHook(memberEventJoin, &member)
			}
		case serf.EventMemberLeave, serf.EventMemberFailed:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member.Name) {
					return
				}

				m.onMemberEventHook(memberEventLeave, &member)
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

func (m *Membership) Broadcast(msgType string, msgPayload interface{}, params *serf.QueryParam) (<-chan BroadcastMessageResp, error) {
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

	respCh := make(chan BroadcastMessageResp, cap(resp.ResponseCh()))

	go func() {
		defer close(respCh)

		for r := range resp.ResponseCh() {
			respCh <- BroadcastMessageResp{
				RequestName: msgType,
				FromID:      r.From,
				Payload:     r.Payload,
			}
		}
	}()

	return respCh, nil
}

func (m *Membership) isLocal(name string) bool {
	return m.serf.LocalMember().Name == name
}

func (m *Membership) IsLeader() bool {
	return m.raft.status == RaftStatusLeader
}

func (m *Membership) Status() RaftStatus {
	return m.raft.status
}

func (m *Membership) AllMembers() Members {
	return m.serf.Members()
}

func (m *Membership) ServiceMembers() Members {
	var mm Members = m.serf.Members()

	if m.serviceName == "" {
		return mm
	}

	return mm.GetService(m.serviceName)
}

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
