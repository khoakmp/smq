package utils

import (
	"math/rand"
)

func RandomSubset(n, k int) []int {
	if k > n {
		panic("k cannot be greater than n")
	}

	// Step 1: Create a slice of [0, 1, 2, ..., n-1]
	nums := make([]int, n)
	for i := 0; i < n; i++ {
		nums[i] = i
	}

	// Step 2: Shuffle it
	rand.Shuffle(n, func(i, j int) {
		nums[i], nums[j] = nums[j], nums[i]
	})

	// Step 3: Take the first k elements
	return nums[:k]
}

var Letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// RandString returns a random string of length n using characters a-z and A-Z
func RandString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = Letters[rand.Intn(len(Letters))]
	}
	return string(b)
}
