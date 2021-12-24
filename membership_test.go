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
	nodesCount := 5
	initPort := int(50000 + rand.Int31n(10000))

	m := setupMember(t, nil, initPort, "")
	for i := 1; i < nodesCount; i++ {
		m = setupMember(t, m, initPort+i, "")
	}

	time.Sleep(60 * time.Second)
}

func TestMembershipManyNodes(t *testing.T) {
	nodesCount := 50
	initPort := int(50000 + rand.Int31n(10000))

	m := setupMember(t, nil, initPort, "")
	for i := 1; i < nodesCount; i++ {
		m = setupMember(t, m, initPort+i, "")
	}

	time.Sleep(60 * time.Second)
}

func TestMembershipTwoServices(t *testing.T) {
	nodesCount := 5
	initPort := int(50000 + rand.Int31n(10000))
	svc := "service1"

	m := setupMember(t, nil, initPort, svc)
	for i := 1; i < nodesCount; i++ {
		if i > nodesCount/2 {
			svc = "service2"
		}

		m = setupMember(t, m, initPort+i, svc)
	}

	time.Sleep(60 * time.Second)
}
