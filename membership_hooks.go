package go_serfly

import (
	"github.com/far4599/go-serfly/types"
	"github.com/hashicorp/serf/serf"
)

// AddMessageListener adds a callback to handle a query message sent by Broadcast method
func (m *Membership) AddMessageListener(msgType string, cb func(*Membership, *serf.Query)) {
	m.messageListeners[msgType] = append(m.messageListeners[msgType], cb)
}

// AddMemberEventListener adds a callback to handle member's raft status change
func (m *Membership) AddMemberEventListener(eventType types.MemberEventType, cb func(*Membership, *serf.Member)) {
	m.memberEventListeners[eventType] = append(m.memberEventListeners[eventType], cb)
}

func (m *Membership) onMessageHook(msgType string, query *serf.Query) {
	for _, fn := range m.messageListeners[msgType] {
		fn(m, query)
	}
}

func (m *Membership) onMemberEventHook(eventType types.MemberEventType, member *serf.Member) {
	if member == nil {
		_m := m.serf.LocalMember()
		member = &_m
	}

	for _, fn := range m.memberEventListeners[eventType] {
		fn(m, member)
	}
}
