package limiter

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ═══════════════════════════════════════════════════════════════════
// Пример из картинки: Ограничитель параллельных запросов
// ═══════════════════════════════════════════════════════════════════

var maxGoroutines = 100

type Request struct {
	ID   int
	Data string
}

// Client имитирует HTTP клиент
type client struct{}

func (c *client) SendRequest(ctx context.Context, req Request) error {
	// Имитация HTTP запроса
	time.Sleep(100 * time.Millisecond)
	fmt.Printf("Запрос %d отправлен: %s\n", req.ID, req.Data)
	return nil
}

// WithLimiter обрабатывает запросы с ограничением количества горутин
func WithLimiter(ctx context.Context, reqs []Request) {
	c := &client{}
	
	// Семафор - канал для ограничения параллелизма
	tokens := make(chan struct{}, maxGoroutines)

	// Заполняем семафор токенами
	go func() {
		for range maxGoroutines {
			tokens <- struct{}{}
		}
	}()

	// Обрабатываем каждый запрос
	for _, req := range reqs {
		<-tokens // Захватываем токен (блокируемся если все заняты)
		
		go func() {
			defer func() {
				tokens <- struct{}{} // Возвращаем токен в пул
			}()

			c.SendRequest(ctx, req)
		}()
	}

	// Ждем завершения всех горутин (забираем все токены обратно)
	for range maxGoroutines {
		<-tokens
	}
}

// ═══════════════════════════════════════════════════════════════════
// Альтернативный вариант с WaitGroup
// ═══════════════════════════════════════════════════════════════════

func WithLimiterAndWaitGroup(ctx context.Context, reqs []Request, maxWorkers int) {
	c := &client{}
	
	// Семафор
	sem := make(chan struct{}, maxWorkers)
	
	// Запускаем все запросы
	for _, req := range reqs {
		req := req // Захват переменной
		
		// Захватываем слот
		sem <- struct{}{}
		
		go func() {
			defer func() { <-sem }() // Освобождаем слот
			
			if err := c.SendRequest(ctx, req); err != nil {
				fmt.Printf("❌ Ошибка в запросе %d: %v\n", req.ID, err)
			}
		}()
	}
	
	// Ждем завершения всех горутин (захватываем все слоты)
	for i := 0; i < cap(sem); i++ {
		sem <- struct{}{}
	}
}

// ═══════════════════════════════════════════════════════════════════
// Вариант с worker pool
// ═══════════════════════════════════════════════════════════════════

func WithWorkerPool(ctx context.Context, reqs []Request, numWorkers int) {
	c := &client{}
	
	// Канал задач
	jobs := make(chan Request, len(reqs))
	
	// Запускаем воркеры
	for w := 1; w <= numWorkers; w++ {
		go func(workerID int) {
			for req := range jobs {
				fmt.Printf("[Worker %d] обрабатывает запрос %d\n", workerID, req.ID)
				c.SendRequest(ctx, req)
			}
		}(w)
	}
	
	// Отправляем задачи в очередь
	for _, req := range reqs {
		jobs <- req
	}
	close(jobs)
	
	// Ждем немного для завершения
	time.Sleep(500 * time.Millisecond)
}

// ═══════════════════════════════════════════════════════════════════
// Rate Limiter с использованием Ticker
// ═══════════════════════════════════════════════════════════════════

var (
	burst = 5   // Максимум burst запросов сразу
	rps   = 10  // Запросов в секунду (requests per second)
)

// WithRateLimiter ограничивает частоту запросов с помощью ticker
func WithRateLimiter(ctx context.Context, reqs []Request) {
	c := &client{}
	
	// Создаем ticker для контроля частоты
	ticker := time.NewTicker(time.Second / time.Duration(rps))
	defer ticker.Stop()
	
	// Канал для токенов (tickets) с буфером для burst
	tickets := make(chan struct{}, burst)
	
	wg := sync.WaitGroup{}
	
	// Заполняем tickets начальным burst'ом
	go func() {
		for range burst {
			tickets <- struct{}{}
		}
	}()
	
	// Пополняем tickets по тикеру (rps раз в секунду)
	go func() {
		for {
			select {
			case <-ticker.C:
				// Неблокирующая отправка (если буфер полон - пропускаем)
				select {
				case tickets <- struct{}{}:
				default:
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	
	// Обрабатываем запросы
	wg.Add(len(reqs))
	for _, req := range reqs {
		req := req // Захват переменной
		<-tickets  // Ждем доступный токен
		
		go func() {
			defer wg.Done()
			c.SendRequest(ctx, req)
		}()
	}
	
	wg.Wait()
}

// ═══════════════════════════════════════════════════════════════════
// Демонстрация всех вариантов
// ═══════════════════════════════════════════════════════════════════

func RunLimiterExamples() {
	fmt.Println("\n╔═══════════════════════════════════════════╗")
	fmt.Println("║      ОГРАНИЧЕНИЕ ПАРАЛЛЕЛЬНЫХ ЗАДАЧ       ║")
	fmt.Println("╚═══════════════════════════════════════════╝")
	
	// Создаем тестовые запросы
	requests := make([]Request, 20)
	for i := 0; i < 20; i++ {
		requests[i] = Request{
			ID:   i + 1,
			Data: fmt.Sprintf("payload-%d", i+1),
		}
	}
	
	ctx := context.Background()
	
	// Вариант 1: Оригинальный с картинки
	fmt.Println("\n=== Вариант 1: Семафор (из картинки) ===")
	fmt.Printf("Ограничение: %d параллельных горутин\n", maxGoroutines)
	startTime := time.Now()
	WithLimiter(ctx, requests[:10])
	fmt.Printf("Время выполнения: %v\n", time.Since(startTime))
	
	// Вариант 2: С WaitGroup
	fmt.Println("\n=== Вариант 2: Семафор с явным контролем ===")
	maxWorkers := 5
	fmt.Printf("Ограничение: %d параллельных воркеров\n", maxWorkers)
	startTime = time.Now()
	WithLimiterAndWaitGroup(ctx, requests[:10], maxWorkers)
	fmt.Printf("Время выполнения: %v\n", time.Since(startTime))
	
	// Вариант 3: Worker Pool
	fmt.Println("\n=== Вариант 3: Worker Pool ===")
	numWorkers := 3
	fmt.Printf("Количество воркеров: %d\n", numWorkers)
	startTime = time.Now()
	WithWorkerPool(ctx, requests[:10], numWorkers)
	fmt.Printf("Время выполнения: %v\n", time.Since(startTime))
	
	// Вариант 4: Rate Limiter с Ticker
	fmt.Println("\n=== Вариант 4: Rate Limiter (с Ticker) ===")
	fmt.Printf("RPS: %d запросов/сек, Burst: %d\n", rps, burst)
	fmt.Println("(Позволяет burst в начале, затем ограничивает по RPS)")
	startTime = time.Now()
	WithRateLimiter(ctx, requests[:15])
	fmt.Printf("Время выполнения: %v\n", time.Since(startTime))
	
	fmt.Println("\n╔═══════════════════════════════════════════╗")
	fmt.Println("║  ИТОГ: Паттерны ограничения параллелизма  ║")
	fmt.Println("╠═══════════════════════════════════════════╣")
	fmt.Println("║ 1. Семафор (chan struct{}) - простой     ║")
	fmt.Println("║ 2. Worker Pool - для долгих задач        ║")
	fmt.Println("║ 3. Rate Limiter - контроль частоты       ║")
	fmt.Println("║ 4. errgroup.SetLimit() - с ошибками      ║")
	fmt.Println("╚═══════════════════════════════════════════╝")
}

