package workerpool

import (
	"fmt"
	"sync"
	"time"
)

// ═══════════════════════════════════════════════════════════════════
// Пример 1: Универсальный Worker Pool с пулом ID воркеров
// ═══════════════════════════════════════════════════════════════════

// makePool создает пул воркеров фиксированного размера
// Возвращает две функции:
// - handle: для добавления задачи в пул
// - wait: для ожидания завершения всех задач
func makePool(poolSize int, handler func(int, string)) (func(string), func()) {
	pool := make(chan int, poolSize)

	// Заполняем пул ID-шниками воркеров
	for i := range poolSize {
		pool <- i
	}

	// handle - функция для добавления задачи
	handle := func(s string) {
		id := <-pool // Захватываем ID воркера из пула
		go func() {
			defer func() {
				pool <- id // Возвращаем ID обратно в пул
			}()

			handler(id, s) // Обрабатываем задачу
		}()
	}

	// wait - функция для ожидания завершения всех воркеров
	wait := func() {
		for range poolSize {
			<-pool // Забираем все ID обратно (все воркеры завершили работу)
		}
	}

	return handle, wait
}

// DemoMakePool демонстрирует работу универсального пула
func DemoMakePool() {
	fmt.Println("\n=== Worker Pool с пулом ID воркеров ===")
	fmt.Println("Создаем пул из 3 воркеров для обработки 10 задач\n")

	// Создаем пул с обработчиком
	handle, wait := makePool(3, func(workerID int, task string) {
		fmt.Printf("[Worker %d] Начинаю обработку: %s\n", workerID, task)
		time.Sleep(200 * time.Millisecond) // Имитация работы
		fmt.Printf("[Worker %d] Завершил: %s\n", workerID, task)
	})

	// Добавляем задачи
	tasks := []string{"Task-1", "Task-2", "Task-3", "Task-4", "Task-5",
		"Task-6", "Task-7", "Task-8", "Task-9", "Task-10"}

	startTime := time.Now()
	for _, task := range tasks {
		handle(task)
	}

	// Ждем завершения всех задач
	wait()
	fmt.Printf("\n✅ Все задачи выполнены за %v\n", time.Since(startTime))
}

// ═══════════════════════════════════════════════════════════════════
// Пример 2: Классический Worker Pool с каналом задач
// ═══════════════════════════════════════════════════════════════════

type Job struct {
	ID   int
	Data string
}

type Result struct {
	JobID  int
	Output string
}

// WorkerPool классический пул воркеров
type WorkerPool struct {
	numWorkers int
	jobs       chan Job
	results    chan Result
	wg         sync.WaitGroup
}

// NewWorkerPool создает новый пул воркеров
func NewWorkerPool(numWorkers int) *WorkerPool {
	return &WorkerPool{
		numWorkers: numWorkers,
		jobs:       make(chan Job, 100),
		results:    make(chan Result, 100),
	}
}

// Start запускает воркеры
func (wp *WorkerPool) Start() {
	for i := 0; i < wp.numWorkers; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}
}

// worker обрабатывает задачи из канала
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	for job := range wp.jobs {
		fmt.Printf("[Worker %d] Обрабатываю Job-%d: %s\n", id, job.ID, job.Data)
		time.Sleep(150 * time.Millisecond) // Имитация работы

		// Отправляем результат
		wp.results <- Result{
			JobID:  job.ID,
			Output: fmt.Sprintf("Processed: %s", job.Data),
		}
	}
}

// Submit добавляет задачу в очередь
func (wp *WorkerPool) Submit(job Job) {
	wp.jobs <- job
}

// Close закрывает канал задач и ждет завершения
func (wp *WorkerPool) Close() {
	close(wp.jobs)
	wp.wg.Wait()
	close(wp.results)
}

// Results возвращает канал результатов
func (wp *WorkerPool) Results() <-chan Result {
	return wp.results
}

// DemoClassicWorkerPool демонстрирует классический Worker Pool
func DemoClassicWorkerPool() {
	fmt.Println("\n=== Классический Worker Pool ===")
	fmt.Println("Пул из 4 воркеров обрабатывает 12 задач\n")

	pool := NewWorkerPool(4)
	pool.Start()

	// Горутина для сбора результатов
	go func() {
		for result := range pool.Results() {
			fmt.Printf("✅ Получен результат Job-%d: %s\n", result.JobID, result.Output)
		}
	}()

	// Добавляем задачи
	startTime := time.Now()
	for i := 1; i <= 12; i++ {
		pool.Submit(Job{
			ID:   i,
			Data: fmt.Sprintf("Data-%d", i),
		})
	}

	// Закрываем пул и ждем завершения
	pool.Close()
	fmt.Printf("\n✅ Все задачи обработаны за %v\n", time.Since(startTime))
}

// ═══════════════════════════════════════════════════════════════════
// Пример 3: Worker Pool с динамическим масштабированием
// ═══════════════════════════════════════════════════════════════════

type DynamicPool struct {
	minWorkers     int
	maxWorkers     int
	currentWorkers int
	tasks          chan func()
	mu             sync.Mutex
	wg             sync.WaitGroup
	done           chan struct{}
	closed         bool
}

// NewDynamicPool создает пул с динамическим количеством воркеров
func NewDynamicPool(minWorkers, maxWorkers int) *DynamicPool {
	dp := &DynamicPool{
		minWorkers:     minWorkers,
		maxWorkers:     maxWorkers,
		currentWorkers: 0,
		tasks:          make(chan func(), 100),
		done:           make(chan struct{}),
	}

	// Запускаем минимальное количество воркеров
	for i := 0; i < minWorkers; i++ {
		dp.addWorker()
	}

	return dp
}

// addWorker добавляет нового воркера
func (dp *DynamicPool) addWorker() {
	dp.mu.Lock()
	if dp.currentWorkers >= dp.maxWorkers {
		dp.mu.Unlock()
		return
	}
	dp.currentWorkers++
	workerID := dp.currentWorkers
	dp.mu.Unlock()

	dp.wg.Add(1)
	go func() {
		defer dp.wg.Done()
		fmt.Printf("🚀 Worker %d запущен\n", workerID)

		for {
			select {
			case task, ok := <-dp.tasks:
				if !ok {
					fmt.Printf("🛑 Worker %d завершен\n", workerID)
					return
				}
				task()
			case <-dp.done:
				fmt.Printf("🛑 Worker %d завершен (сигнал done)\n", workerID)
				return
			}
		}
	}()
}

// Submit добавляет задачу
func (dp *DynamicPool) Submit(task func()) {
	dp.tasks <- task
}

// Shutdown завершает работу пула
func (dp *DynamicPool) Shutdown() {
	dp.mu.Lock()
	if !dp.closed {
		dp.closed = true
		close(dp.tasks)
	}
	dp.mu.Unlock()
	dp.wg.Wait()
}

// DemoDynamicPool демонстрирует динамический пул
func DemoDynamicPool() {
	fmt.Println("\n=== Динамический Worker Pool ===")
	fmt.Println("Начинаем с 2 воркеров, максимум 5\n")

	pool := NewDynamicPool(2, 5)

	startTime := time.Now()

	// Добавляем задачи
	for i := 1; i <= 8; i++ {
		taskID := i
		pool.Submit(func() {
			fmt.Printf("📦 Обработка задачи %d\n", taskID)
			time.Sleep(300 * time.Millisecond)
			fmt.Printf("✅ Задача %d завершена\n", taskID)
		})
	}

	fmt.Printf("\n✅ Все задачи отправлены, ждем завершения...\n")
	pool.Shutdown()
	fmt.Printf("✅ Все задачи выполнены за %v\n", time.Since(startTime))
}

// ═══════════════════════════════════════════════════════════════════
// Пример 4: Worker Pool с приоритетами
// ═══════════════════════════════════════════════════════════════════

type PriorityJob struct {
	Priority int
	Task     func()
}

type PriorityPool struct {
	numWorkers int
	highPrio   chan func()
	lowPrio    chan func()
	wg         sync.WaitGroup
}

// NewPriorityPool создает пул с поддержкой приоритетов
func NewPriorityPool(numWorkers int) *PriorityPool {
	pp := &PriorityPool{
		numWorkers: numWorkers,
		highPrio:   make(chan func(), 50),
		lowPrio:    make(chan func(), 50),
	}

	// Запускаем воркеры
	for i := 0; i < numWorkers; i++ {
		pp.wg.Add(1)
		go pp.worker(i)
	}

	return pp
}

// worker обрабатывает задачи с учетом приоритета
func (pp *PriorityPool) worker(id int) {
	defer pp.wg.Done()

	for {
		select {
		case task, ok := <-pp.highPrio:
			if !ok {
				return
			}
			fmt.Printf("[Worker %d] 🔴 HIGH priority task\n", id)
			task()
		default:
			select {
			case task, ok := <-pp.highPrio:
				if !ok {
					return
				}
				fmt.Printf("[Worker %d] 🔴 HIGH priority task\n", id)
				task()
			case task, ok := <-pp.lowPrio:
				if !ok {
					return
				}
				fmt.Printf("[Worker %d] 🟢 LOW priority task\n", id)
				task()
			}
		}
	}
}

// SubmitHigh добавляет задачу с высоким приоритетом
func (pp *PriorityPool) SubmitHigh(task func()) {
	pp.highPrio <- task
}

// SubmitLow добавляет задачу с низким приоритетом
func (pp *PriorityPool) SubmitLow(task func()) {
	pp.lowPrio <- task
}

// Close закрывает пул
func (pp *PriorityPool) Close() {
	close(pp.highPrio)
	close(pp.lowPrio)
	pp.wg.Wait()
}

// DemoPriorityPool демонстрирует пул с приоритетами
func DemoPriorityPool() {
	fmt.Println("\n=== Worker Pool с приоритетами ===")
	fmt.Println("3 воркера обрабатывают задачи с разными приоритетами\n")

	pool := NewPriorityPool(3)

	startTime := time.Now()

	// Добавляем задачи с разными приоритетами
	for i := 1; i <= 5; i++ {
		taskID := i
		pool.SubmitLow(func() {
			time.Sleep(100 * time.Millisecond)
			fmt.Printf("   ✅ Low priority task %d завершена\n", taskID)
		})
	}

	time.Sleep(50 * time.Millisecond)

	// Добавляем высокоприоритетные задачи
	for i := 1; i <= 3; i++ {
		taskID := i
		pool.SubmitHigh(func() {
			time.Sleep(100 * time.Millisecond)
			fmt.Printf("   ✅ High priority task %d завершена\n", taskID)
		})
	}

	pool.Close()
	fmt.Printf("\n✅ Все задачи выполнены за %v\n", time.Since(startTime))
}

// ═══════════════════════════════════════════════════════════════════
// Демонстрация всех вариантов
// ═══════════════════════════════════════════════════════════════════

func RunWorkerPoolExamples() {
	fmt.Println("\n╔═══════════════════════════════════════════╗")
	fmt.Println("║         WORKER POOL ПАТТЕРНЫ              ║")
	fmt.Println("╚═══════════════════════════════════════════╝")

	// Пример 1: Универсальный пул с ID воркеров
	DemoMakePool()

	// Пример 2: Классический Worker Pool
	DemoClassicWorkerPool()

	// Пример 3: Динамический пул
	DemoDynamicPool()

	// Пример 4: Пул с приоритетами
	DemoPriorityPool()

	fmt.Println("\n╔═══════════════════════════════════════════╗")
	fmt.Println("║  ИТОГ: Worker Pool паттерны               ║")
	fmt.Println("╠═══════════════════════════════════════════╣")
	fmt.Println("║ 1. Универсальный с пулом ID               ║")
	fmt.Println("║ 2. Классический с каналом задач           ║")
	fmt.Println("║ 3. Динамический с масштабированием        ║")
	fmt.Println("║ 4. С приоритетами задач                   ║")
	fmt.Println("╚═══════════════════════════════════════════╝")
}
