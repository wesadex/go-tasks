package main

import (
	"fmt"
	"time"
)

// --- Provided API (do not implement) ---

// increment atomically increments the global counter by 1 and returns the new value.
// Thread-safe but expensive — avoid calling unnecessarily.
func increment() int64 { return 0 }

// get_count returns the current counter value.
func get_count() int64 { return 0 }

// reset_count atomically resets the counter to 0.
func reset_count() {}

// --- Implement --- //

type entry struct {
}

type RateLimiter struct {
	// TODO
}

// NewRateLimiter creates a limiter.
// mode: "fixed_window" or "sliding_window"
// limit: max allowed requests per window
// windowSize: window duration
func NewRateLimiter(mode string, limit int, windowSize time.Duration) *RateLimiter {
	// TODO
}

// Allow checks if a request from the given clientID should be allowed.
// Returns true if within limit, false if throttled.
func (r *RateLimiter) Allow(clientID string) bool {
	// TODO
}

// Stats returns current request counts per client for the active window.
func (r *RateLimiter) Stats() map[string]int {
	// TODO
}

func main() {
	rl := NewRateLimiter("fixed_window", 3, 5*time.Second)

	fmt.Println(rl.Allow("alice")) // true  (1/3)
	fmt.Println(rl.Allow("alice")) // true  (2/3)
	fmt.Println(rl.Allow("alice")) // true  (3/3)
	fmt.Println(rl.Allow("alice")) // false (throttled)
	fmt.Println(rl.Allow("bob"))   // true  (bob has own counter)

	time.Sleep(5 * time.Second)
	fmt.Println(rl.Allow("alice")) // true  (new window)
}
