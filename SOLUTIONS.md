# Go tasks solutions

## #8

```go
func predictableFunc(ctx context.Context) (int, error) {
	ch := make(chan struct{})
	var result int

	go func() {
		result = unpredictableFunc()
		close(ch)
	}()

	select {
	case <-ch:
		return result, nil
	case <-ctx.Done():
		return 0, errors.New("timed out")
	}
}
```

## #10

```go
package main

import "sync"

// реализовать функцию
func fanin(chans ...<-chan int) <-chan int {
	result := make(chan int)
	wg := sync.WaitGroup{}

	go func() {
		for _, ch := range chans {
			wg.Add(1)
			go func() {
				defer wg.Done()

				for val := range ch {
					result <- val
				}
			}()
		}

		wg.Wait()
		close(result)
	}()

	return result
}
```

## #11

### Решение без errgroup

```go
func fetch(ctx context.Context, user User) (string, error) {
	if user.Name == "Ann" {
		return "", errors.New("invalid name")
	}

	ch := make(chan any)

	go func() {
		time.Sleep(time.Millisecond * 10)
		close(ch)
	}()

	select {
	case <-ch:
		return user.Name, nil
	case <-ctx.Done():
		return "", errors.New("context canceled")
	}
}


func process(ctx context.Context, users []User) (map[string]int64, error) {
	names := make(map[string]int64, 0)
	mu := sync.Mutex{}
	wg := sync.WaitGroup{}

	wg.Add(len(users))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var commonError error
	for _, u := range users {
		go func() {
			defer wg.Done()

			name, err := fetch(ctx, u)
			if err != nil {
				sync.OnceFunc(func() {
					cancel()
					commonError = err
				})()
			}

			mu.Lock()
			defer mu.Unlock()
			names[name] = names[name] + 1
		}()
	}

	wg.Wait()

	if commonError != nil {
		return nil, commonError
	}

	return names, nil
}
```

### Решение с errgroup

```go
func process(ctx context.Context, users []User) (map[string]int64, error) {
	names := make(map[string]int64, 0)
	mu := sync.Mutex{}

	egroup, ectx := errgroup.WithContext(ctx)


	egroup.SetLimit(100) // Maximum 100 goroutines

	for _, u := range users {
		egroup.Go(func() error {
			name, err := fetch(ectx, u)
			if err != nil {
				return err
			}

			mu.Lock()
			defer mu.Unlock()
			names[name] = names[name] + 1

			return nil
		})
	}

	if err := egroup.Wait(); err != nil {
		return nil, err
	}

	return names, nil
}
```

## #12

### maxConnects limit
```go
func generate(reqs []Request) chan Request {
	ch := make(chan Request)

	go func() {
		for _, v := range reqs {
			ch <- v
		}
		close(ch)
	}()

	return ch
}

var maxConnects = 10 // merely amount of workers-goroutines
func (c client) WithLimiter(ctx context.Context, ch chan Request) {
	wg := sync.WaitGroup{}
	wg.Add(maxConnects)

	for range maxConnects {
		go func() {
			defer wg.Done()

			for req := range ch {
				c.SendRequest(ctx, req)
			}
		}()
	}

	wg.Wait()
}

func main() {
	ctx := context.Background()
	c := client{}
	requests := make([]Request, 1000)
	for i := 0; i < 1000; i++ {
		requests[i] = Request{Payload: strconv.Itoa(i)}
	}
	c.WithLimiter(ctx, generate(requests))
}
```

### max goroutine limit

```go
var maxGoroutines = 100

func (c client) WithLimiter(ctx context.Context, reqs []Request) {
	tokens := make(chan struct{}, maxGoroutines)

	go func() {
		for range maxGoroutines {
			tokens <- struct{}{} // generate tokens for limiting goroutine number
		}
	}()

	for _, req := range reqs {
		<-tokens
		go func() {
			defer func() {
				tokens <- struct{}{}
			}()

			c.SendRequest(ctx, req)
		}()
	}

	for range maxGoroutines {
		<-tokens // read all the tokens for sync instead of wait group or so
	}
}

func main() {
	ctx := context.Background()
	c := client{}
	requests := make([]Request, 1000)
	for i := 0; i < 1000; i++ {
		requests[i] = Request{Payload: strconv.Itoa(i)}
	}
	c.WithLimiter(ctx, requests) // just requests, not channel
}

```

### max RPS limit

#### Simple one

```go
var rps = 100

func (c client) WithLimiter(ctx context.Context, reqs []Request) {
	ticker := time.NewTicker(time.Second / time.Duration(rps))

	wg := sync.WaitGroup{}
	wg.Add(len(reqs))

	for _, req := range reqs {
		<-ticker.C
		go func() {
			defer wg.Done()
			c.SendRequest(ctx, req)
		}()
	}

	wg.Wait()
}
```

#### With burst (some initial pre-allowed amount of requests we can send before RPS limitation)

```go
var rps = 1
var burst = 10

func (c client) WithLimiter(ctx context.Context, reqs []Request) {
	ticker := time.NewTicker(time.Second / time.Duration(rps))
	tickets := make(chan struct{}, burst)

	wg := sync.WaitGroup{}

	go func() {
		for range burst {
			tickets <- struct{}{}
		}
	}()

	go func() {
		for {
			select {
			case <-ticker.C:
				tickets <- struct{}{}
			}
		}
	}()

	wg.Add(len(reqs))
	for _, req := range reqs {
		<-tickets
		go func() {
			defer wg.Done()
			c.SendRequest(ctx, req)
		}()
	}

	wg.Wait()
}
```

And also we can limit time for each request:

```go
for _, req := range reqs {
	<-tickets
	go func() {
		defer wg.Done()

		ctx, cancel := context.WithTimeout(ctx, 5*time.Second) // 5 second for each, otherwise - cancel by timeout inside sendRequest()
		defer cancel()

		c.SendRequest(ctx, req)
	}()
}
```

## #13

### Without limiters

```go
func process(urls []string) map[int]int {
	statusCodeCounts := make(map[int]int)
	wg := sync.WaitGroup{}

	wg.Add(len(urls))

	for _, url := range urls {
		go func() {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)

			resp, err := client.Do(req)
			if err != nil {
				fmt.Println("error occured:", err.Error())
				return
			}

			statusCodeCounts[resp.StatusCode]++ // concurrent write in map!!!
		}()
	}

	wg.Wait()
	return statusCodeCounts
}
```

### With limiter

```go
var maxConnects = 100

func process(urls []string) map[int]int {
	statusCodeCounts := make(map[int]int)
	wg := sync.WaitGroup{}
	ch := make(chan string)

	go func() {
		for _, url := range urls {
			ch <- url
		}
		close(ch)
	}()

	processUrl := func(url string) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		resp, err := client.Do(req)
		if err != nil {
			fmt.Println("error occured:", err.Error())
			return
		}

		statusCodeCounts[resp.StatusCode]++ // concurrent write in map!!!
	}

	wg.Add(maxConnects)
	for range maxConnects {
		go func() {
			defer wg.Done()

			for url := range ch {
				processUrl(url)
			}
		}()
	}

	wg.Wait()
	return statusCodeCounts
}
```

## #14 - Worker pool (true pool)

```go
package main

import (
	"fmt"
	"time"
)

func worker(id int, phrase string) {
	time.Sleep(100 * time.Millisecond)
	fmt.Printf("Worker %d says: %s\n", id, phrase)
}

func makePool(poolSize int, handler func(int, string)) (func(string), func()) {
	pool := make(chan int, poolSize)
	for i := range poolSize {
		pool <- i
	}

	handle := func(s string) {
		id := <-pool
		go func() {
			defer func() {
				pool <- id
			}()
			handler(id, s)
		}()
	}

	wait := func() {
		for range poolSize {
			<-pool
		}
	}

	return handle, wait
}

func main() {
	phrases := []string{}

	for i := range 100 {
		phrases = append(phrases, fmt.Sprintf("phrase %d", i))
	}

	handle, wait := makePool(5, worker)

	for _, phrase := range phrases {
		handle(phrase)
	}

	wait()

	fmt.Println("Done!")
}
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
// Simple token-bucket rate limiter (per destination)
// ---------------------------------------------------------------------------

// rateLimiter implements a basic token bucket. Workers call Wait() before
// executing a job — it blocks until a token is available, enforcing the
// per-destination rate without any coordination between workers.
type rateLimiter struct {
	tokens chan struct{}
	done   chan struct{}
}

func newRateLimiter(ratePerSec int) *rateLimiter {
	rl := &rateLimiter{
		tokens: make(chan struct{}, ratePerSec), // burst = ratePerSec
		done:   make(chan struct{}),
	}

	// Refill one token every 1/rate seconds.
	go func() {
		interval := time.Second / time.Duration(ratePerSec)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Non-blocking put: if bucket is full, drop the token.
				select {
				case rl.tokens <- struct{}{}:
				default:
				}
			case <-rl.done:
				return
			}
		}
	}()

	// Pre-fill the bucket so the first burst doesn't wait.
	for i := 0; i < ratePerSec; i++ {
		rl.tokens <- struct{}{}
	}

	return rl
}

// Wait blocks until a token is available or ctx is cancelled.
func (rl *rateLimiter) Wait(ctx context.Context) error {
	select {
	case <-rl.tokens:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (rl *rateLimiter) Stop() {
	close(rl.done)
}

// ---------------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------------

var ErrSchedulerStopped = errors.New("scheduler is stopped, not accepting new jobs")

const jobQueueBufferSize = 256

type Scheduler struct {
	jobCh    chan Job                  // single shared channel for all workers
	limiters map[string]*rateLimiter  // per-destination rate limiters
	stopped  atomic.Bool
	wg       sync.WaitGroup           // tracks in-flight jobs
	workerWg sync.WaitGroup           // tracks worker goroutines
}

func NewScheduler(configs []DestinationConfig, workerCount int) *Scheduler {
	s := &Scheduler{
		jobCh:    make(chan Job, jobQueueBufferSize),
		limiters: make(map[string]*rateLimiter, len(configs)),
	}

	// Create a rate limiter for each destination.
	for _, cfg := range configs {
		s.limiters[cfg.Name] = newRateLimiter(cfg.MaxPerSec)
	}

	// Launch exactly workerCount goroutines — each pulls from the
	// shared channel and can handle ANY destination.
	s.workerWg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go s.worker(i)
	}

	return s
}

// worker is a generic worker that picks up any job from the shared queue,
// waits for the per-destination rate limiter, then executes.
func (s *Scheduler) worker(id int) {
	defer s.workerWg.Done()

	for job := range s.jobCh {
		limiter := s.limiters[job.Destination]

		// Block until the destination's rate limiter grants a token.
		// This is the key point: multiple workers may be waiting on
		// the SAME limiter concurrently — the token bucket serializes
		// them at the correct rate.
		if err := limiter.Wait(context.Background()); err != nil {
			fmt.Printf("[WARN] worker %d: rate limit wait cancelled for job %s: %v\n", id, job.ID, err)
			s.wg.Done()
			continue
		}

		if err := job.Execute(); err != nil {
			fmt.Printf("[ERROR] worker %d: job %s to %s failed: %v\n", id, job.ID, job.Destination, err)
		}

		s.wg.Done()
	}
}

func (s *Scheduler) Submit(job Job) error {
	if s.stopped.Load() {
		return ErrSchedulerStopped
	}

	if _, ok := s.limiters[job.Destination]; !ok {
		return fmt.Errorf("unknown destination: %s", job.Destination)
	}

	job.submittedAt = time.Now()

	// Track before enqueue so Shutdown always accounts for it.
	s.wg.Add(1)

	select {
	case s.jobCh <- job:
		return nil
	default:
		s.wg.Done()
		return fmt.Errorf("job queue is full (backpressure)")
	}
}

// Shutdown stops accepting new jobs, waits for in-flight jobs, then
// tears down workers and rate limiters.
func (s *Scheduler) Shutdown(ctx context.Context) error {
	// 1. Reject new submissions.
	s.stopped.Store(true)

	// 2. Wait for all enqueued jobs to finish.
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All jobs drained — now close channel so workers exit.
	case <-ctx.Done():
		return fmt.Errorf("shutdown timed out waiting for jobs: %w", ctx.Err())
	}

	// 3. Close job channel so workers' range loops exit.
	close(s.jobCh)
	s.workerWg.Wait()

	// 4. Stop rate limiter refill goroutines.
	for _, rl := range s.limiters {
		rl.Stop()
	}

	return nil
}

// ---------------------------------------------------------------------------
// Demo
// ---------------------------------------------------------------------------

func main() {
	configs := []DestinationConfig{
		{Name: "service-a", MaxPerSec: 5},
		{Name: "service-b", MaxPerSec: 10},
		{Name: "service-c", MaxPerSec: 2},
	}

	sched := NewScheduler(configs, 8) // 8 workers, any can handle any destination

	start := time.Now()

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
		fmt.Printf("All jobs completed, clean shutdown. Took %v\n", time.Since(start))
	}
}
```

# Algorhitms

## Task 1

```go
func topKFrequent(nums []int, k int) []int {
	type entry struct {
        key  int
        freq int
    }

	m := make(map[int]int)

	for _, num := range nums {
		m[num]++
	}

    entries := make([]entry, 0, len(m))
    for key, freq := range m {
        entries = append(entries, entry{key, freq})
    }

    sort.Slice(entries, func(i, j int) bool {
        return entries[i].freq > entries[j].freq
    })

    result := make([]int, 0, k)

	for i := 0; i < k; i++ {
		result = append(result, entries[i].key)
	}

	return result
}

```

## Task 2 - BTree

```go
package main

import "fmt"

type TreeNode struct {
	Val   int
	Right *TreeNode
	Left  *TreeNode
}

var ErrNotFound = "element is not found"

func findInBTree(root *TreeNode, n int) (int, error) {
	found := false
	num := n
	var nextNode *TreeNode = root

	for {
		if nextNode == nil {
			if found {
				return num, nil
			} else {
				return 0, fmt.Errorf(ErrNotFound)
			}
		}

		if nextNode.Val > n {
			num = nextNode.Val
			found = true
			nextNode = nextNode.Left
		} else {
			nextNode = nextNode.Right
		}
	}
}

func main() {
	root := &TreeNode{
		Val: 10,
		Left: &TreeNode{
			Val:   5,
			Right: &TreeNode{Val: 7},
			Left:  &TreeNode{Val: 2},
		},
		Right: &TreeNode{
			Val:  15,
			Left: &TreeNode{Val: 12},
			Right: &TreeNode{
				Val:   18,
				Left:  &TreeNode{Val: 17},
				Right: &TreeNode{Val: 19},
			},
		},
	}
	num, err := findInBTree(root, 10)
	fmt.Printf("Result: %d, Error: %v", num, err)
}
```

## Task3 - Return error without fmt and errors usage

```go
package main

import "fmt"

type MyError struct {
}

func (m *MyError) Error() string {
	return "My Error"
}

func test() error {
	var a *MyError
	return a
}

func main() {
	fmt.Printf("%v", test())
}
```

## Task4 - Encode-decode string

```go
package main

import (
	"fmt"
	"strconv"
	"strings"
)

type Solution struct{}

func (s *Solution) Encode(strs []string) string {
	var b strings.Builder
	for _, str := range strs {
		b.WriteString(fmt.Sprintf("%d#", len(str)))
		b.WriteString(str)
	}
	println(b.String())
	return b.String()
}

func (s *Solution) Decode(encoded string) []string {
	result := []string{}
	i := 0
	for i < len(encoded) {
		j := i
		for encoded[j] != '#' {
			j++
		}
		ln, _ := strconv.Atoi(encoded[i:j])
		result = append(result, encoded[j+1:j+1+ln])
		i = j + 1 + ln
	}
	return result
}

func main() {
	var sol Solution

	s := sol.Encode([]string{"Fucking", "piece", "of", "shit"})
	fmt.Printf("%s", s)

	fmt.Printf("%v", sol.Decode(s))

}
```

## Task5 - Return product of int elements but current

```go
func productExceptSelf(nums []int) []int {
	product := 1
	res := make([]int, len(nums))
	zeroes := 0

	for _, num := range nums {
		if num == 0 {
			zeroes++
		} else {
			product *= num
		}
	}

	if zeroes > 1 {
		return res
	}

	for i, num := range nums {
		if num == 0 && zeroes == 1 {
			res[i] = product
		}

		if zeroes == 0 {
			res[i] = product / num
		}
	}

	return res
}
```

### BETTER!

```go
func productExceptSelf(nums []int) []int {
    n := len(nums)
    result := make([]int, n)

    prefix := 1
    for i := 0; i < n; i++ {
        result[i] = prefix
        prefix *= nums[i]
    }

    suffix := 1
    for i := n - 1; i >= 0; i-- {
        result[i] *= suffix
        suffix *= nums[i]
    }

    return result
}
```

## Task6 - Longest consequitive

```go
func longestConsecutive(nums []int) int {
    if len(nums) == 0 {
        return 0
    }

    m := make(map[int]struct{})
    for _, num := range nums {
        m[num] = struct{}{}
    }

    best := 1
    for _, num := range nums {
        if _, ok := m[num-1]; ok {
            continue
        }
        ln := 1
        for {
            if _, ok := m[num+ln]; ok {
                ln++
            } else {
                break
            }
        }
        if ln > best {
            best = ln
        }
    }
    return best
}
```

## Task7 - Is palindrome?

```go
func isAlphaNumeric(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
}
func isPalindrome(s string) bool {
	s = strings.ToLower(s)
	i, j := 0, len(s)-1
	for {
		if i >= j {
			break
		}
		for i < j && !isAlphaNumeric(s[i]) {
			i++
		}
		for i < j && !isAlphaNumeric(s[j]) {
			j--
		}
		if s[i] != s[j] {
			return false
		}
		i++
		j--
	}
	return true
}
```

## Task8 - Find items by sum

```go
func twoSum(numbers []int, target int) []int {
    i, j := 0, len(numbers)-1
    for i < j {
        sum := numbers[i] + numbers[j]
        if sum == target {
            return []int{i + 1, j + 1}
        } else if sum < target {
            i++
        } else {
            j--
        }
    }
    return []int{}
}
```

## Task9 - 3 Sum

Идея в том, чтобы свести 3 sum к 2 sum, поэтому в первом цикле мы просто перебираем все элементы до len(nums)-2, а во втором - 2 sum по оставщимся в пределах "i - len(nums)-1". Везде проверяем дубликаты.

```go
func threeSum(nums []int) [][]int {
	sort.Ints(nums) // n * log(n)
	res := [][]int{}
	for i := 0; i < len(nums)-2; i++ { // O(n^2)
		// looking for duplicates for i
		if i > 0 && nums[i] == nums[i-1] {
 		   continue
		}

		l,r := i+1, len(nums)-1
		for l < r {
			sum := nums[i] + nums[l] + nums[r]
			if sum == 0 {
				res = append(res, []int{nums[i], nums[l], nums[r]})
				// looking for duplicates for l,r
				for l < r && nums[l] == nums[l+1] {
					l++
				}
				for l < r && nums[r] == nums[r-1] {
					r--
				}
				l++
				r--
			} else if sum < 0 {
				l++
			} else {
				r--
			}
		}
	}
	return res // space O(1) не считая результата
}

```

## Task 10 - пиздец с собеса

```go
type timerEntry struct {
	id       uuid.UUID
	callback func()
	deadline time.Time
	canceled bool
}

type MultipleTimers struct {
	mu      sync.Mutex
	entries map[uuid.UUID]*timerEntry
	// отсортированный слайс или heap по deadline
	queue   []*timerEntry // для простоты слайс, в проде — heap
	activeID uuid.UUID    // id текущего физического таймера
}

func NewMultipleTimers() *MultipleTimers {
	return &MultipleTimers{
		entries: make(map[uuid.UUID]*timerEntry),
	}
}

func (m *MultipleTimers) delayed_callback(cb func(), delay int) uuid.UUID {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := uuid.New()
	entry := &timerEntry{
		id:       id,
		callback: cb,
		deadline: time.Now().Add(time.Duration(delay) * time.Millisecond),
	}
	m.entries[id] = entry
	m.insertSorted(entry)

	// Если новый — самый ранний, перенастраиваем физический таймер
	if m.queue[0] == entry {
		m.resetPhysicalTimer()
	}

	return id
}

func (m *MultipleTimers) cancel_callback(id uuid.UUID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if e, ok := m.entries[id]; ok {
		e.canceled = true
		// Если отменили ближайший — перенастраиваем
		if len(m.queue) > 0 && m.queue[0] == e {
			m.queue = m.queue[1:]
			m.resetPhysicalTimer()
		}
	}
}

func (m *MultipleTimers) get_callback_time(id uuid.UUID) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	if e, ok := m.entries[id]; ok && !e.canceled {
		remaining := time.Until(e.deadline).Milliseconds()
		if remaining < 0 {
			return 0
		}
		return int(remaining)
	}
	return 0
}

func (m *MultipleTimers) resetPhysicalTimer() {
	// Пропускаем отменённые
	for len(m.queue) > 0 && m.queue[0].canceled {
		m.queue = m.queue[1:]
	}
	if len(m.queue) == 0 {
		return
	}

	// Отменяем предыдущий физический таймер
	cancel_callback(m.activeID)

	next := m.queue[0]
	delay := int(time.Until(next.deadline).Milliseconds())
	if delay < 0 {
		delay = 0
	}

	m.activeID = delayed_callback(func() {
		m.onFire()
	}, delay)
}

func (m *MultipleTimers) onFire() {
	m.mu.Lock()
	// Собираем все готовые колбэки
	var ready []func()
	now := time.Now()
	for len(m.queue) > 0 && !m.queue[0].deadline.After(now) {
		e := m.queue[0]
		m.queue = m.queue[1:]
		if !e.canceled {
			ready = append(ready, e.callback)
		}
		delete(m.entries, e.id)
	}
	// Перенастраиваем на следующий
	m.resetPhysicalTimer()
	m.mu.Unlock()

	// Вызываем колбэки вне лока
	for _, cb := range ready {
		cb()
	}
}
```