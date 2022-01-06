package go_serfly

import (
	"crypto/rand"
	"math/big"
)

// randInt64n generates random int64 in range [0,n]
func randInt64n(n int64) int64 {
	if n <= 0 {
		return 0
	}

	res, _ := rand.Int(rand.Reader, big.NewInt(n))
	return res.Int64()
}
