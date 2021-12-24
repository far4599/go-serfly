package go_serfly

import "github.com/hashicorp/serf/serf"

type memberEventType int

const (
	memberEventJoin memberEventType = iota
	memberEventLeave
	memberEventBecameFollower
	memberEventBecameCandidate
	memberEventBecameLeader
)

func (m *Membership) AddMessageListener(msgType string, cb func(*Membership, *serf.Query)) {
	m.messageListeners[msgType] = append(m.messageListeners[msgType], cb)
}

func (m *Membership) AddMemberEventListener(eventType memberEventType, cb func(*Membership, *serf.Member)) {
	m.memberEventListeners[eventType] = append(m.memberEventListeners[eventType], cb)
}

func (m *Membership) onMessageHook(msgType string, query *serf.Query) {
	for _, fn := range m.messageListeners[msgType] {
		fn(m, query)
	}
}

func (m *Membership) onMemberEventHook(eventType memberEventType, member *serf.Member) {
	for _, fn := range m.memberEventListeners[eventType] {
		fn(m, member)
	}
}
