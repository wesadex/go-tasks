package atomic

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Имитация API
var (
	nodeAddedTime2 = make(map[string]time.Time)
	nodeReadyTime2 = make(map[string]time.Duration)
	nodeMutex2     sync.Mutex
)

func AddNode2(name string) error {
	time.Sleep(100 * time.Millisecond)
	nodeMutex2.Lock()
	nodeAddedTime2[name] = time.Now()
	nodeReadyTime2[name] = time.Duration(2+time.Now().UnixNano()%7) * time.Second
	nodeMutex2.Unlock()
	fmt.Printf("✅ Нода %s добавлена\n", name)
	return nil
}

func IsNodeReady2(name string) bool {
	nodeMutex2.Lock()
	addedTime, exists := nodeAddedTime2[name]
	readyDuration := nodeReadyTime2[name]
	nodeMutex2.Unlock()

	if !exists {
		return false
	}
	return time.Since(addedTime) >= readyDuration
}

// ═══════════════════════════════════════════════════════════════════
// СПОСОБ 1: Только каналы (без WaitGroup)
// ═══════════════════════════════════════════════════════════════════

func DeployWithChannelsOnly(nodeNames []string, timeout time.Duration) error {
	fmt.Println("\n╔══════════════════════════════════════════╗")
	fmt.Println("║ СПОСОБ 1: Только каналы (БЕЗ WaitGroup) ║")
	fmt.Println("╚══════════════════════════════════════════╝\n")

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Шаг 1: Добавляем ноды
	fmt.Println("=== Добавление нод ===")
	doneCh := make(chan bool)
	errorCh := make(chan error, len(nodeNames))

	for _, name := range nodeNames {
		go func(nodeName string) {
			if err := AddNode2(nodeName); err != nil {
				errorCh <- err
			}
			doneCh <- true // Сигнал о завершении
		}(name)
	}

	// Ждем завершения всех добавлений
	completed := 0
	for completed < len(nodeNames) {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout при добавлении нод")
		case err := <-errorCh:
			return err
		case <-doneCh:
			completed++
		}
	}

	// Шаг 2: Ждем готовности
	fmt.Println("\n=== Ожидание готовности ===")
	readyCh := make(chan string)

	for _, name := range nodeNames {
		go func(nodeName string) {
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if IsNodeReady2(nodeName) {
						fmt.Printf("🟢 Нода %s готова!\n", nodeName)
						readyCh <- nodeName
						return
					}
				}
			}
		}(name)
	}

	// Подсчитываем готовые ноды
	ready := 0
	for ready < len(nodeNames) {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout: %d/%d нод готовы", ready, len(nodeNames))
		case <-readyCh:
			ready++
		}
	}

	fmt.Printf("✅ Все %d нод готовы!\n", len(nodeNames))
	return nil
}

// ═══════════════════════════════════════════════════════════════════
// СПОСОБ 2: Atomic counter (атомарный счетчик)
// ═══════════════════════════════════════════════════════════════════

func DeployWithAtomic(nodeNames []string, timeout time.Duration) error {
	fmt.Println("\n╔══════════════════════════════════════════╗")
	fmt.Println("║ СПОСОБ 2: Атомарный счетчик             ║")
	fmt.Println("╚══════════════════════════════════════════╝\n")

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var addedCount int32 = 0 // Атомарный счетчик
	errorCh := make(chan error, 1)

	// Шаг 1: Добавляем ноды
	fmt.Println("=== Добавление нод ===")
	for _, name := range nodeNames {
		go func(nodeName string) {
			if err := AddNode2(nodeName); err != nil {
				select {
				case errorCh <- err:
				default:
				}
				return
			}
			atomic.AddInt32(&addedCount, 1) // Атомарно увеличиваем
		}(name)
	}

	// Ждем пока все добавятся
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout при добавлении")
		case err := <-errorCh:
			return err
		default:
			if atomic.LoadInt32(&addedCount) == int32(len(nodeNames)) {
				goto ready_check // Все добавлены!
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

ready_check:
	// Шаг 2: Ждем готовности
	fmt.Println("\n=== Ожидание готовности ===")
	var readyCount int32 = 0
	readyCh := make(chan string)

	for _, name := range nodeNames {
		go func(nodeName string) {
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if IsNodeReady2(nodeName) {
						fmt.Printf("🟢 Нода %s готова!\n", nodeName)
						atomic.AddInt32(&readyCount, 1)
						readyCh <- nodeName
						return
					}
				}
			}
		}(name)
	}

	// Ждем готовности всех
	for {
		select {
		case <-ctx.Done():
			ready := atomic.LoadInt32(&readyCount)
			return fmt.Errorf("timeout: %d/%d готовы", ready, len(nodeNames))
		case <-readyCh:
			if atomic.LoadInt32(&readyCount) == int32(len(nodeNames)) {
				fmt.Printf("✅ Все %d нод готовы!\n", len(nodeNames))
				return nil
			}
		}
	}
}

// ═══════════════════════════════════════════════════════════════════
// СПОСОБ 3: Единый канал для сбора результатов
// ═══════════════════════════════════════════════════════════════════

type NodeResult struct {
	Name  string
	Error error
}

func DeployWithResultChannel(nodeNames []string, timeout time.Duration) error {
	fmt.Println("\n╔══════════════════════════════════════════╗")
	fmt.Println("║ СПОСОБ 3: Единый канал результатов      ║")
	fmt.Println("╚══════════════════════════════════════════╝\n")

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Один канал для ВСЕХ результатов
	resultCh := make(chan NodeResult, len(nodeNames))

	// Шаг 1: Добавляем ноды
	fmt.Println("=== Добавление нод ===")
	for _, name := range nodeNames {
		go func(nodeName string) {
			err := AddNode2(nodeName)
			resultCh <- NodeResult{Name: nodeName, Error: err}
		}(name)
	}

	// Собираем результаты добавления
	addedNodes := []string{}
	for i := 0; i < len(nodeNames); i++ {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout при добавлении")
		case result := <-resultCh:
			if result.Error != nil {
				return result.Error
			}
			addedNodes = append(addedNodes, result.Name)
		}
	}

	// Шаг 2: Ждем готовности
	fmt.Println("\n=== Ожидание готовности ===")
	for _, name := range addedNodes {
		go func(nodeName string) {
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if IsNodeReady2(nodeName) {
						fmt.Printf("🟢 Нода %s готова!\n", nodeName)
						resultCh <- NodeResult{Name: nodeName}
						return
					}
				}
			}
		}(name)
	}

	// Собираем результаты готовности
	ready := 0
	for ready < len(addedNodes) {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout: %d/%d готовы", ready, len(addedNodes))
		case <-resultCh:
			ready++
		}
	}

	fmt.Printf("✅ Все %d нод готовы!\n", len(addedNodes))
	return nil
}

// ═══════════════════════════════════════════════════════════════════
// СПОСОБ 4: Семафор (ограничение параллелизма)
// ═══════════════════════════════════════════════════════════════════

func DeployWithSemaphore(nodeNames []string, timeout time.Duration, maxParallel int) error {
	fmt.Println("\n╔══════════════════════════════════════════╗")
	fmt.Println("║ СПОСОБ 4: Семафор (лимит параллелизма)  ║")
	fmt.Println("╚══════════════════════════════════════════╝\n")

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Семафор - буферизованный канал
	semaphore := make(chan struct{}, maxParallel)
	doneCh := make(chan bool, len(nodeNames))
	errorCh := make(chan error, 1)

	// Шаг 1: Добавляем ноды с ограничением параллелизма
	fmt.Printf("=== Добавление нод (макс. %d параллельно) ===\n", maxParallel)
	for _, name := range nodeNames {
		go func(nodeName string) {
			// Захватываем слот
			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }() // Освобождаем слот
			case <-ctx.Done():
				return
			}

			if err := AddNode2(nodeName); err != nil {
				select {
				case errorCh <- err:
				default:
				}
				return
			}
			doneCh <- true
		}(name)
	}

	// Ждем завершения
	completed := 0
	for completed < len(nodeNames) {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout")
		case err := <-errorCh:
			return err
		case <-doneCh:
			completed++
		}
	}

	fmt.Printf("✅ Все %d нод добавлены!\n", len(nodeNames))
	return nil
}

// ═══════════════════════════════════════════════════════════════════
// СРАВНЕНИЕ ПОДХОДОВ
// ═══════════════════════════════════════════════════════════════════

func CompareApproaches() {
	fmt.Println(`
╔═══════════════════════════════════════════════════════════════════╗
║                     СРАВНЕНИЕ ПОДХОДОВ                            ║
╚═══════════════════════════════════════════════════════════════════╝

1. WaitGroup (классический):
   ✅ Простой и понятный
   ✅ Стандартный подход
   ✅ Меньше кода
   ❌ Нужна отдельная синхронизация для ошибок

2. Только каналы:
   ✅ Явная коммуникация
   ✅ Не нужен WaitGroup
   ❌ Больше кода для подсчета

3. Атомарный счетчик (atomic):
   ✅ Быстрее WaitGroup (меньше блокировок)
   ✅ Хорош для простого подсчета
   ❌ Нужно polling (проверка в цикле)
   ❌ Сложнее для понимания

4. Единый канал результатов:
   ✅ Централизованная обработка
   ✅ Легко добавить метаданные
   ❌ Больше памяти (буфер)

5. Семафор:
   ✅ Контроль параллелизма
   ✅ Защита от перегрузки
   ❌ Дополнительная сложность

РЕКОМЕНДАЦИЯ:
- Простая задача → WaitGroup
- Нужен контроль параллелизма → Семафор
- Много метаданных → Канал результатов
- Высокая производительность → Atomic
`)
}

func RunWithoutWaitGroup() {
	nodeNames := []string{
		"node-1", "node-2", "node-3", "node-4",
		"node-5", "node-6", "node-7", "node-8",
	}
	timeout := 30 * time.Second

	// Способ 1: Только каналы
	if err := DeployWithChannelsOnly(nodeNames, timeout); err != nil {
		fmt.Printf("❌ Ошибка: %v\n", err)
	}

	time.Sleep(2 * time.Second)

	// Способ 2: Атомарный счетчик
	if err := DeployWithAtomic(nodeNames, timeout); err != nil {
		fmt.Printf("❌ Ошибка: %v\n", err)
	}

	time.Sleep(2 * time.Second)

	// Способ 3: Единый канал
	if err := DeployWithResultChannel(nodeNames, timeout); err != nil {
		fmt.Printf("❌ Ошибка: %v\n", err)
	}

	time.Sleep(2 * time.Second)

	// Способ 4: Семафор (только 3 ноды параллельно)
	if err := DeployWithSemaphore(nodeNames[:4], timeout, 2); err != nil {
		fmt.Printf("❌ Ошибка: %v\n", err)
	}

	// Сравнение
	CompareApproaches()
}
