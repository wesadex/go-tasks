# Go tasks

Solutions are [here](SOLUTIONS.md)

## #1

Что выведет код?
Как исправить?

```go
package main

import "fmt"

func main() {
	for i := 0; i < 100; i++ {
		go func() {
			fmt.Println(i)
		}()
	}
}
```

## #2

Что выведет код?
Как исправить?

```go
package main

import "fmt"

func main() {
	counter := 0
	for i := 0; i < 100; i++ {
		go func() {
			counter++
		}()
	}
	fmt.Println(counter)
}
```

## #3

Рассказать, что будет выведено на экран.

```go
package main

import (
	"fmt"
	"runtime"
	"sync"
	"time"
)
// 
func worker(wg *sync.WaitGroup) {
	defer wg.Done()
	time.Sleep(1 * time.Millisecond)
}

func main() {
	runtime.GOMAXPROCS(1)

	MAX_TASKS := 10_000

	wg := &sync.WaitGroup{}
	wg.Add(MAX_TASKS)

	start := time.Now()
	for range MAX_TASKS {
		go worker(wg)
	}

	wg.Wait()
	fmt.Println(time.Since(start))
}
```

## #4

Что выведет на экран?

```go
package main

import (
	"fmt"
	"time"
)

func worker() <-chan int {
	ch := make(chan int)
	go func() {
		time.Sleep(1 * time.Second)
		close(ch)
	}()
	return ch
}

func main() {
	start := time.Now()
	_, _ = worker(), worker()
    // Второй вариант
    //_, _ = <-worker(), <-worker()
	
    fmt.Println(time.Since(start))
}
```

## #5

Что произойдет, что будет выведено на экран?

```go
package main

import "fmt"

func main() {
	ch := make(chan int)

	go func() {
		for i := 0; i < 100; i++ {
			ch <- i
		}
	}()

	for n := range ch {
		fmt.Println(n)
	}
}
```

## #6

Что произойдет, что будет выведено на экран?

```go
package main

import "fmt"

func spawnMessages(n int) chan string {
	ch := make(chan string, 1)
	for i := 0; i < n; i++ {
		ch <- fmt.Sprintf("msg %d", i+1)
	}
	return ch
}

func main() {
	n := 10
	for msg := range spawnMessages(n) {
		fmt.Println("received:", msg)
	}
}
```

## #7

Что будет выведено на экран?

```go
package main

import "fmt"

func main() {
	ch := make(chan int, 1)

	for i := 0; i < 5; i++ {
		select {
		case val := <-ch:
			fmt.Println(val)
		case ch <- i:
		}
	}
}
```

## #8 - Predictable and unpredictabls funcs

Реализовать predictableFunc()

```go
package main

import (
	"math/rand"
	"time"
)

func unpredictableFunc() int {
	n := rand.Intn(40)
	time.Sleep(time.Duration(n) * time.Second)
	return n
}

func predictableFunc() int {

}

func main() {
	_ = predictableFunc()
}
```

## #9 - Channel with channels

Что произойдет?

```go
package main

import "fmt"

type c chan c

func main() {
	var c = make(c, 1)
	c <- c
	for i := 0; i < 1000; i++ {
		select {
		case <-c:
		case <-c:
			c <- c
		default:
			fmt.Println(i)
			return
		}
	}
}
```

## #10 - Fanin (gather info from lots of channels, process it and send to one channel)

Реализовать функцию

```go
package main

// реализовать функцию
func fanin(chans ...<-chan int) <-chan int {
}
```

## #11 - errgroup

- Что происходит, что выведется, будет ли ошибка?
- Распараллелить запросы. Как только произойдет ошибка, чтобы все запросы останавливались. ErrorGroup!

```go
package main

import (
	"context"
	"fmt"
	"time"
)

type User struct {
	Name string
}

func fetch(ctx context.Context, user User) (string, error) {
	time.Sleep(time.Millisecond * 10)
	return user.Name, nil
}

func process(ctx context.Context, users []User) (map[string]int64, error) {
	names := make(map[string]int64, 0)

	for _, u := range users {
		name, err := fetch(ctx, u)
		if err != nil {
		}

		names[name] = names[name] + 1
	}

	return names, nil
}

func main() {
	names := []User{
		{"Ann"},
		{"Bob"},
		{"Cindy"},
		{"Bob"},
	}

	ctx := context.Background()

	start := time.Now()
	res, err := process(ctx, names)
	if err != nil {
		fmt.Println("an error occured:", err.Error())
	}
	fmt.Println("time:", time.Since(start))
	fmt.Println(res)
}
```

## #12 Rate-limiter

Реализовать WithLimiter() в трех вариантах:
- Goroutine limit
- RPS limit
- Connects limit

```go

package main

import (
	"context"
	"fmt"
	"time"
)

type Request struct {
	Payload string
}

type Client interface {
	SendRequest(ctx context.Context, request Request) error
	WithLimiter(ctx context.Context, requests []Request)
}

type client struct {
}

func (c client) SendRequest(ctx context.Context, request Request) error {
	time.Sleep(100 * time.Millisecond)
	fmt.Println("sending request", request.Payload)
	return nil
}

func (c client) WithLimiter(ctx context.Context, requests []Request) {
}

func main() {
	ctx := context.Background()
	c := client{}
	requests := make([]Request, 1000)
	for i := 0; i < 1000; i++ {
		requests[i] = Request{Payload: strconv.Itoa(i)}
	}
	c.WithLimiter(ctx, requests)
}
```

## #13 Process URLs in parallel

```go
package main

func main() {
	urls := []string{
		"https://google.com",
		"https://yandex.ru",
		"https://amazon.com",
		"https://youtube.com",
	}

	process(urls)
}

// реализовать параллельные запросы по адресам из списка
// подсчитать количество для каждого StatusCode ответа
// предусмотреть возможность отмены запросов по таймауту
func process(urls []string) {
}
```

## #14 - Worker pool, TRUE!

Реализовать функцию makePool()

```go
package main

import (
	"fmt"
	"time"
)

func say(id int, phrase string) {
	time.Sleep(20 * time.Millisecond)
	fmt.Printf("Worker %d says: %s\n", id, phrase)
}

func makePool(poolSize int, handler func(int, string)) (func(string), func()) {
}

func main() {
	phrases := []string{}
	for i := range 100 {
		phrases = append(phrases, fmt.Sprintf("phrase %d", i))
	}

	handle, wait := makePool(5, say)

	for _, phrase := range phrases {
		handle(phrase)
	}

	wait()

	fmt.Println("Done!")
}
```

## #15 - in-memory cache

```go
package main

// Необходимо реализовать in-memory кэш, который реализует операции добавления,
// поиска и удаления элемента за минимальное время. Кэш должен быть
// конкурентно-безопасным, пригодным для использования в многопоточной среде.
```

## #16 Task scheduler

```go
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

type Job struct {
	ID          string
	Destination string
	Payload     []byte
	submittedAt time.Time
}

func (j Job) Execute() error {
	time.Sleep(50 * time.Millisecond)
	return nil
}

type DestinationConfig struct {
	Name      string
	MaxPerSec int
}

// ---------------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------------

var ErrSchedulerStopped = errors.New("scheduler is stopped, not accepting new jobs")

// Core design decision: one goroutine per destination.
//
// Each destination gets a buffered channel and a dedicated goroutine
// that drains it at the configured rate using time.Ticker.
//
// Why this approach:
//   - Rate limiting is trivial: one tick = one permit to call Execute().
//   - Jobs to the same destination are naturally serialized — no mutexes.
//   - Different destinations run fully concurrently in separate goroutines.
//   - Backpressure: if the channel buffer fills up, Submit blocks (or we
//     can return an error — here I chose to return an error for non-blocking).
//
// Trade-off: we ignore the workerCount param in favor of one-goroutine-per-dest.
// A worker-pool design is also valid but adds complexity for per-dest rate limits.

const perDestBufferSize = 256

type destinationQueue struct {
	ch     chan Job
	ticker *time.Ticker
}

type Scheduler struct {
	queues  map[string]*destinationQueue
	stopped atomic.Bool
	wg      sync.WaitGroup // tracks in-flight jobs across all destinations
}

func NewScheduler(configs []DestinationConfig, workerCount int) *Scheduler {
	s := &Scheduler{
		queues: make(map[string]*destinationQueue, len(configs)),
	}

	for _, cfg := range configs {
		dq := &destinationQueue{
			ch:     make(chan Job, perDestBufferSize),
			ticker: time.NewTicker(time.Second / time.Duration(cfg.MaxPerSec)),
		}
		s.queues[cfg.Name] = dq

		// Launch one goroutine per destination that respects the rate limit.
		go s.runDestination(dq)
	}

	return s
}

// runDestination drains a single destination's job channel, executing one job
// per tick of the rate-limit ticker.
func (s *Scheduler) runDestination(dq *destinationQueue) {
	defer dq.ticker.Stop()

	for job := range dq.ch {
		// Wait for the next tick — this enforces the per-second rate.
		<-dq.ticker.C

		// Execute the job.
		if err := job.Execute(); err != nil {
			// Part 1: just log. Part 2 will add retries.
			fmt.Printf("[ERROR] job %s to %s failed: %v\n", job.ID, job.Destination, err)
		}

		// Signal that this in-flight job is done.
		s.wg.Done()
	}
}

func (s *Scheduler) Submit(job Job) error {
	if s.stopped.Load() {
		return ErrSchedulerStopped
	}

	dq, ok := s.queues[job.Destination]
	if !ok {
		return fmt.Errorf("unknown destination: %s", job.Destination)
	}

	job.submittedAt = time.Now()

	// Track this job as in-flight BEFORE sending to channel,
	// so Shutdown's wg.Wait() accounts for it.
	s.wg.Add(1)

	// Non-blocking send: if the buffer is full, report backpressure.
	select {
	case dq.ch <- job:
		return nil
	default:
		s.wg.Done() // undo the Add — job was never enqueued
		return fmt.Errorf("destination %s queue is full (backpressure)", job.Destination)
	}
}

// Shutdown stops accepting new jobs, waits for all in-flight jobs to complete,
// and respects the context deadline.
func (s *Scheduler) Shutdown(ctx context.Context) error {
	// 1. Stop accepting new submissions.
	s.stopped.Store(true)

	// 2. Close all channels so destination goroutines will drain and exit.
	for _, dq := range s.queues {
		close(dq.ch)
	}

	// 3. Wait for in-flight jobs OR context cancellation.
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("shutdown timed out: %w", ctx.Err())
	}
}

// ---------------------------------------------------------------------------
// Quick demo
// ---------------------------------------------------------------------------

func main() {
	configs := []DestinationConfig{
		{Name: "service-a", MaxPerSec: 5},
		{Name: "service-b", MaxPerSec: 10},
		{Name: "service-c", MaxPerSec: 2},
	}

	sched := NewScheduler(configs, 8)

	for i := 0; i < 30; i++ {
		dest := fmt.Sprintf("service-%c", 'a'+rune(i%3))
		err := sched.Submit(Job{
			ID:          fmt.Sprintf("job-%d", i),
			Destination: dest,
			Payload:     []byte(`{"event":"user.created"}`),
		})
		if err != nil {
			fmt.Printf("[WARN] submit: %v\n", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := sched.Shutdown(ctx); err != nil {
		fmt.Printf("[ERROR] shutdown: %v\n", err)
	} else {
		fmt.Println("All jobs completed, clean shutdown.")
	}
}
```

## #17 LRU cache

Implement a thread-safe LRU cache in Go with bound capacity.


### The Task
We need a rate-limited job scheduler that dispatches webhook notifications to external services. Each external service has its own rate limit.
Part 1 (the only part for now): Implement NewScheduler, Submit, and Shutdown.
The rules are simple:

Jobs arrive via Submit() and specify a Destination (e.g. "service-a")
Each destination has a max requests-per-second limit (given in config)
Jobs to the same destination must not exceed that rate
Jobs should be processed concurrently across different destinations
Shutdown must wait for in-flight jobs to finish, but reject new submissions

# Algorhitms

## Task 1

Given an integer array nums and an integer k, return the k most frequent elements within the array.

The test cases are generated such that the answer is always unique.

You may return the output in any order.

Example 1:

Input: nums = [1,2,2,3,3,3], k = 2

Output: [2,3]

Example 2:

Input: nums = [7,7], k = 1

Output: [7]

Constraints:

    1 <= nums.length <= 10^4.
    -1000 <= nums[i] <= 1000
    1 <= k <= number of distinct elements in nums.

You should aim for a solution with O(n) time and O(n) space, where n is the size of the input array. 

## Task 2 - BTree

Write a function which takes a tree and a number as an input. It should find in the tree and output minimum number that is greater than given.
Example of tree:
             10
           /    \
         5       15
       /  |      /  \
      2   7    12   18
                     /\
                   17 19
n = 16
expected output is 17

## Task 3 - return error without fmt and errors usage

```go
package main

import "fmt"

func test() error {
// implement
}

func main() {
	fmt.Printf("%v", test())
}
```

## Task4 - Encode-decode string

Design an algorithm to encode a list of strings to a string. The encoded string is then sent over the network and is decoded back to the original list of strings.

Machine 1 (sender) has the function:

string encode(vector<string> strs) {
    // ... your code
    return encoded_string;
}

Machine 2 (receiver) has the function:

vector<string> decode(string s) {
    //... your code
    return strs;
}

So Machine 1 does:

string encoded_string = encode(strs);

and Machine 2 does:

vector<string> strs2 = decode(encoded_string);

strs2 in Machine 2 should be the same as strs in Machine 1.

Implement the encode and decode methods.

## Task5 - Return product of int elements but current

Given an integer array nums, return an array output where output[i] is the product of all the elements of nums except nums[i].

Each product is guaranteed to fit in a 32-bit integer.

Follow-up: Could you solve it in O(n)O(n) time without using the division operation?

Example 1:

Input: nums = [1,2,4,6]

Output: [48,24,12,8]

Example 2:

Input: nums = [-1,0,1,2,3]

Output: [0,-6,0,0,0]

Constraints:

    2 <= nums.length <= 1000
    -20 <= nums[i] <= 20

## Task6 - Longest consequitive

Given an array of integers nums, return the length of the longest consecutive sequence of elements that can be formed.
A consecutive sequence is a sequence of elements in which each element is exactly 1 greater than the previous element. The elements do not have to be consecutive in the original array.
You must write an algorithm that runs in O(n) time.

## Task7 - Is palindrome?

Given a string s, return true if it is a palindrome, otherwise return false.

A palindrome is a string that reads the same forward and backward. It is also case-insensitive and ignores all non-alphanumeric characters.

Note: Alphanumeric characters consist of letters (A-Z, a-z) and numbers (0-9).

Example 1:

Input: s = "Was it a car or a cat I saw?"

Output: true

Explanation: After considering only alphanumerical characters we have "wasitacaroracatisaw", which is a palindrome.

Example 2:

Input: s = "tab a cat"

Output: false

Explanation: "tabacat" is not a palindrome.

Constraints:

    1 <= s.length <= 1000
    s is made up of only printable ASCII characters.


## Task8 - find items by sum

Given an array of integers numbers that is sorted in non-decreasing order.

Return the indices (1-indexed) of two numbers, [index1, index2], such that they add up to a given target number target and index1 < index2. Note that index1 and index2 cannot be equal, therefore you may not use the same element twice.

There will always be exactly one valid solution.

Your solution must use O(1)O(1) additional space.

Example 1:

Input: numbers = [1,2,3,4], target = 3

Output: [1,2]

Explanation:
The sum of 1 and 2 is 3. Since we are assuming a 1-indexed array, index1 = 1, index2 = 2. We return [1, 2].

Constraints:

    2 <= numbers.length <= 1000
    -1000 <= numbers[i] <= 1000
    -1000 <= target <= 1000

## Task9 - 3 Sum

Given an integer array nums, return all the triplets [nums[i], nums[j], nums[k]] where nums[i] + nums[j] + nums[k] == 0, and the indices i, j and k are all distinct.

The output should not contain any duplicate triplets. You may return the output and the triplets in any order.

Example 1:

Input: nums = [-1,0,1,2,-1,-4]

Output: [[-1,-1,2],[-1,0,1]]

Explanation:
nums[0] + nums[1] + nums[2] = (-1) + 0 + 1 = 0.
nums[1] + nums[2] + nums[4] = 0 + 1 + (-1) = 0.
nums[0] + nums[3] + nums[4] = (-1) + 2 + (-1) = 0.
The distinct triplets are [-1,0,1] and [-1,-1,2].

Example 2:

Input: nums = [0,1,1]

Output: []

Explanation: The only possible triplet does not sum up to 0.

Example 3:

Input: nums = [0,0,0]

Output: [[0,0,0]]

Explanation: The only possible triplet sums up to 0.

Constraints:

    3 <= nums.length <= 1000
    -10^5 <= nums[i] <= 10^5

## Tasks 10 - пиздец с собеса

```go
package main

import (
	"time"

	"github.com/google/uuid"
)

// --- Provided single-timer API (do not implement) ---

// delayed_callback schedules callback to be called after delay milliseconds.
// Uses a shared object internally — cannot be called simultaneously by multiple goroutines.
// Returns a UUID identifying the scheduled callback.
func delayed_callback(callback func(), delay int) uuid.UUID { return uuid.UUID{} }

// cancel_callback cancels the scheduled callback identified by id.
func cancel_callback(id uuid.UUID) {}

// get_callback_time returns the remaining time in ms before the callback is triggered.
func get_callback_time(id uuid.UUID) int { return 0 }

// --- Your implementation ---

// MultipleTimers supports scheduling multiple independent delayed callbacks
//

type MultipleTimers struct {
}

// delayed_callback schedules callback to fire after delay milliseconds.
// Returns a UUID that can be used to cancel or query the callback.
func (m *MultipleTimers) delayed_callback(callback func(), delay int) uuid.UUID {
	// TODO
}

// cancel_callback cancels the callback identified by id.
func (m *MultipleTimers) cancel_callback(id uuid.UUID) {
  // todo
}

// get_callback_time returns the remaining time in ms before the callback fires.
func (m *MultipleTimers) get_callback_time(id uuid.UUID) int {
    // TODO
    return 0
}
```
Реализовать MultipleTimers.
