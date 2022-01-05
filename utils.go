package go_serfly

import (
	"crypto/rand"
	"math"
	"math/big"
	"time"
)

// randInt64n generates random int64 in range [0,n]
func randInt64n(n int64) int64 {
	if n <= 0 {
		return 0
	}

	res, _ := rand.Int(rand.Reader, big.NewInt(n))
	return res.Int64()
}

// queryTimeout calculates approximate timeout for serf query among n members
// to receive response from 100% of members
func queryTimeout(membersCount int) time.Duration {
	timeout := 200 * time.Millisecond
	timeout *= time.Duration(6)
	timeout *= time.Duration(math.Ceil(math.Log10(float64(membersCount + 1))))
	return timeout
}
