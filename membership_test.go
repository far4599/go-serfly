package go_serfly

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/hashicorp/serf/serf"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func setupMember(t *testing.T, members []*Membership, port int, serviceName string) []*Membership {
	id := len(members)
	addr := fmt.Sprintf("%s:%d", "127.0.0.1", port)
	c := Config{
		NodeName: fmt.Sprintf("%d", id),
		BindAddr: addr,
	}

	if len(members) > 0 {
		c.KnownClusterAddresses = []string{
			members[0].BindAddr,
		}
	}

	startedAt := time.Now()

	on := func(id, role string) func(*Membership, *serf.Member) {
		return func(m *Membership, _ *serf.Member) {
			svc := m.serviceName
			if svc != "" {
				svc = svc + " node:"
			}

			if role == "leader" {
				fmt.Printf("%s%s became %s on %s\n", svc, id, role, time.Now().Sub(startedAt).String())

				//go func() {
				//	time.Sleep(5 * time.Second)
				//
				//	i, _ := strconv.ParseInt(id, 10, 64)
				//
				//	fmt.Printf("%s left cluster at %s\n", id, time.Now().Sub(startedAt).String())
				//
				//	err := members[i].Stop()
				//	require.NoError(t, err)
				//}()
			}
		}
	}

	i := strconv.Itoa(id)

	//logger, err := zap.NewDevelopment()
	//require.NoError(t, err)

	logger := zap.NewNop()

	m, err := New(c, WithOnBecomeLeaderCallback(on(i, "leader")), WithOnBecomeFollowerCallback(on(i, "follower")), WithOnBecomeCandidateCallback(on(i, "candidate")), WithLogger(logger), WithServiceName(serviceName))
	require.NoError(t, err)

	err = m.Serve()
	require.NoError(t, err)

	members = append(members, m)

	return members
}

func TestMembershipThreeNodes(t *testing.T) {
	nodesCount := 3
	initPort := int(50000 + rand.Int31n(10000))

	m := setupMember(t, nil, initPort, "")
	for i := 1; i < nodesCount; i++ {
		m = setupMember(t, m, initPort+i, "")
	}

	// check if leader is elected
	require.Eventually(t, func() bool {
		return m[0].ServiceMembers().GetLeader() != nil
	}, 60*time.Second, 250*time.Millisecond)

	time.Sleep(3 * time.Second)

	leader := m[0].ServiceMembers().GetLeader()
	for _, membership := range m {
		if membership.NodeName == leader.Name {
			fmt.Printf("leader %s leaves the cluster\n", leader.Name)

			// leader leaves the cluster
			err := membership.Stop()
			require.NoError(t, err)

			break
		}
	}

	// check if a new leader is elected
	require.Eventually(t, func() bool {
		return m[0].ServiceMembers().GetLeader() != nil
	}, 60*time.Second, 250*time.Millisecond)
}

func TestMembershipManyNodes(t *testing.T) {
	nodesCount := 30 // the more nodes in the cluster, the longer test will last
	initPort := int(50000 + rand.Int31n(10000))

	m := setupMember(t, nil, initPort, "")
	for i := 1; i < nodesCount; i++ {
		m = setupMember(t, m, initPort+i, "")
	}

	// check if leader is elected
	require.Eventually(t, func() bool {
		return m[0].ServiceMembers().GetLeader() != nil
	}, 60*time.Second, 250*time.Millisecond)
}

func TestMembershipThreeServices(t *testing.T) {
	nodesCount := 3
	serviceCount := 3
	initPort := int(50000 + rand.Int31n(10000))
	serviceMap := make(map[string]struct{}, serviceCount)

	var m []*Membership
	p := 0
	for i := 0; i < serviceCount; i++ {
		s := fmt.Sprintf("%s-%d", "service", i)
		serviceMap[s] = struct{}{}

		for j := 0; j < nodesCount; j++ {
			m = setupMember(t, m, initPort+p, s)
			p++
		}
	}

	// check if all services elected leader
	require.Eventually(t, func() bool {
		result := true

		for s := range serviceMap {
			if m[0].AllMembers().GetService(s).GetLeader() == nil {
				result = false
			}
		}

		return result
	}, 30*time.Second, 250*time.Millisecond)
}
