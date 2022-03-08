package go_serfly

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/far4599/go-serfly/transport"
	"github.com/hashicorp/serf/serf"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
)

func TestMembershipTestSuite(t *testing.T) {
	suite.Run(t, new(MembershipTestSuite))
}

type MembershipTestSuite struct {
	suite.Suite

	lastUsedPort   int
	portProbeMutex sync.Mutex
	logger         *zap.Logger
}

func (s *MembershipTestSuite) SetupSuite() {
	s.logger, _ = zap.NewDevelopment()
	s.lastUsedPort = 10000
}

func (s *MembershipTestSuite) setupMember(members []*Membership, serviceName string, startedAt time.Time, opts []opt) []*Membership {
	id := len(members)
	addr := fmt.Sprintf("%s:%d", "127.0.0.1", s.getNextFreePort())
	c := Config{
		Name: fmt.Sprintf("%d", id),
		Addr: addr,
	}

	if len(members) > 0 {
		c.KnownClusterAddresses = []string{
			members[0].Addr,
		}
	}

	onBecameEvent := func(id, role string) func(*Membership, *serf.Member) {
		return func(m *Membership, _ *serf.Member) {
			svc := m.serviceName
			if svc != "" {
				svc = svc + " node:"
			}

			if role == "leader" {
				svc = "==> " + svc
			}

			s.logger.Debug(fmt.Sprintf("%s%s became %s on %s\n", svc, id, role, time.Now().Sub(startedAt).String()))
		}
	}

	onMemberEvent := func(id, action string) func(*Membership, *serf.Member) {
		return func(m *Membership, _ *serf.Member) {
			//svc := m.serviceName
			//if svc != "" {
			//	svc = svc + " node:"
			//}
			//
			//a := "joined"
			//if action == "leave" {
			//	a = "left"
			//}
			//
			//logger.Debug(fmt.Sprintf("%s%s %s cluster on %s\n", svc, id, a, time.Now().Sub(startedAt).String()))
		}
	}

	i := strconv.Itoa(id)

	tr := transport.NewHttpTransport("127.0.0.1", s.getNextFreePort(), nil, s.logger)

	basicOpts := []opt{WithServiceName(serviceName), WithLogger(s.logger), WithSerfLogger(zap.NewNop()), WithOnBecomeLeaderCallback(onBecameEvent(i, "leader")), WithOnBecomeFollowerCallback(onBecameEvent(i, "follower")), WithOnBecomeCandidateCallback(onBecameEvent(i, "candidate")), WithOnMemberJoinCallback(onMemberEvent(i, "join")), WithOnMemberLeaveCallback(onMemberEvent(i, "leave"))}

	opts = append(basicOpts, opts...)

	m, err := NewMembership(c, tr, opts...)
	s.Require().NoError(err)

	err = m.Serve()
	s.Require().NoError(err)

	members = append(members, m)

	return members
}

func (s *MembershipTestSuite) getNextFreePort() int {
	s.portProbeMutex.Lock()
	defer s.portProbeMutex.Unlock()

	for port := s.lastUsedPort + 1; port < (1 << 16); port++ {
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			continue
		}
		_ = ln.Close()

		s.lastUsedPort = port
		return port
	}

	return -1
}

// TestMembershipOneNode tests that if there is only one node it will never become a leader
func (s *MembershipTestSuite) TestMembershipOneNode() {
	s.T().Parallel()

	nodesCount := 1
	startedAt := time.Now()

	m := s.setupMember(nil, "", startedAt, nil)
	for i := 1; i < nodesCount; i++ {
		m = s.setupMember(m, "", startedAt, nil)
	}

	// check leader must never not be elected
	s.Require().Never(func() bool {
		return m[0].ServiceMembers().GetLeader() != nil
	}, 5*time.Second, 250*time.Millisecond)
}

// TestMembershipOneNode tests that if there is only one node it will never become a leader
func (s *MembershipTestSuite) TestMembershipOneNodeWithEnabledSingleNodeCanBecomeLeader() {
	s.T().Parallel()

	nodesCount := 1
	startedAt := time.Now()

	m := s.setupMember(nil, "", startedAt, []opt{WithSingleNodeCanBecomeLeader()})
	for i := 1; i < nodesCount; i++ {
		m = s.setupMember(m, "", startedAt, nil)
	}

	// check leader must never not be elected
	s.Require().Eventually(func() bool {
		return m[0].ServiceMembers().GetLeader() != nil
	}, 5*time.Second, 250*time.Millisecond)
}

func (s *MembershipTestSuite) TestMembershipTwoNodes() {
	s.T().Parallel()

	nodesCount := 2
	startedAt := time.Now()

	m := s.setupMember(nil, "", startedAt, nil)
	for i := 1; i < nodesCount; i++ {
		m = s.setupMember(m, "", startedAt, nil)
	}

	// check if leader is elected
	s.Require().Eventually(func() bool {
		return m[0].ServiceMembers().GetLeader() != nil
	}, 5*time.Second, 250*time.Millisecond)
}

// TestMembershipThreeNodes will create three node cluster, then the leader will leave the cluster after 1 sec from been elected, and a new leader must be elected
func (s *MembershipTestSuite) TestMembershipThreeNodes() {
	s.T().Parallel()

	nodesCount := 3
	startedAt := time.Now()

	m := s.setupMember(nil, "", startedAt, nil)
	for i := 1; i < nodesCount; i++ {
		m = s.setupMember(m, "", startedAt, nil)
	}

	sampleM := m[0]

	// check if leader is elected
	s.Require().Eventually(func() bool {
		return sampleM.ServiceMembers().GetLeader() != nil
	}, 5*time.Second, 250*time.Millisecond)

	time.Sleep(1 * time.Second)

	leader := m[0].ServiceMembers().GetLeader()
	for _, membership := range m {
		if membership.Name == leader.Name {
			fmt.Printf("leader %s leaves the cluster\n", leader.Name)

			// leader leaves the cluster
			err := membership.Stop()
			s.Require().NoError(err)

			if sampleM == membership {
				sampleM = m[1]
			}

			break
		}
	}

	// check if a new leader is elected
	s.Require().Eventually(func() bool {
		return sampleM.ServiceMembers().GetLeader() != nil
	}, 5*time.Second, 250*time.Millisecond)
}

func (s *MembershipTestSuite) TestMembershipManyNodes() {
	s.T().Parallel()

	nodesCount := 15 // the more nodes in the cluster, the longer test will last
	startedAt := time.Now()

	m := s.setupMember(nil, "", startedAt, nil)
	for i := 1; i < nodesCount; i++ {
		m = s.setupMember(m, "", startedAt, nil)
	}

	// check if leader is elected
	s.Require().Eventually(func() bool {
		return m[0].ServiceMembers().GetLeader() != nil
	}, 5*time.Second, 250*time.Millisecond)
}

func (s *MembershipTestSuite) TestMembershipThreeServices() {
	s.T().Parallel()

	nodesCount := 3
	startedAt := time.Now()

	serviceCount := 5
	serviceMap := make(map[string]struct{}, serviceCount)

	var m []*Membership
	p := 0
	for i := 0; i < serviceCount; i++ {
		svc := fmt.Sprintf("%s-%d", "service", i)
		serviceMap[svc] = struct{}{}

		for j := 0; j < nodesCount; j++ {
			m = s.setupMember(m, svc, startedAt, nil)
			p++
		}
	}

	// check if all services elected leader
	s.Require().Eventually(func() bool {
		result := true

		for s := range serviceMap {
			if m[0].AllMembers().GetService(s).GetLeader() == nil {
				result = false
			}
		}

		return result
	}, 5*time.Second, 250*time.Millisecond)
}
