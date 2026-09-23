package waitgroup

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ============================================
// Имитация API для работы с нодами
// ============================================

var (
	nodeAddedTime = make(map[string]time.Time)     // Когда нода была добавлена
	nodeReadyTime = make(map[string]time.Duration) // Через сколько нода станет готова
	nodeMutex     sync.Mutex
)

// AddNode добавляет ноду с указанным именем
func AddNode(name string) error {
	fmt.Printf("⏳ Добавление ноды %s...\n", name)
	time.Sleep(100 * time.Millisecond) // Имитация сетевого запроса

	// Запоминаем время добавления и случайное время готовности (2-8 секунд)
	nodeMutex.Lock()
	nodeAddedTime[name] = time.Now()
	nodeReadyTime[name] = time.Duration(2+time.Now().UnixNano()%7) * time.Second
	nodeMutex.Unlock()

	fmt.Printf("✅ Нода %s добавлена (станет готова через %v)\n", name, nodeReadyTime[name])
	return nil
}

// IsNodeReady проверяет готовность ноды
func IsNodeReady(name string) bool {
	nodeMutex.Lock()
	addedTime, exists := nodeAddedTime[name]
	readyDuration := nodeReadyTime[name]
	nodeMutex.Unlock()

	if !exists {
		return false // Нода не была добавлена
	}

	// Проверяем, прошло ли достаточно времени
	elapsed := time.Since(addedTime)
	return elapsed >= readyDuration
}

// ============================================
// РЕШЕНИЕ 1: Базовое с WaitGroup
// ============================================

func AddNodesParallel(nodeNames []string) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(nodeNames)) // Буферизованный канал для ошибок

	// Добавляем все ноды ПАРАЛЛЕЛЬНО
	for _, name := range nodeNames {
		wg.Add(1) // Увеличиваем счетчик WaitGroup
		go func(nodeName string) {
			defer wg.Done() // Уменьшаем счетчик при завершении горутины

			// Пытаемся добавить ноду
			if err := AddNode(nodeName); err != nil {
				errChan <- fmt.Errorf("failed to add node %s: %w", nodeName, err)
			}
		}(name) // Передаем name как параметр, чтобы избежать race condition
	}

	// Ждем завершения ВСЕХ горутин
	wg.Wait()
	close(errChan) // Закрываем канал после завершения всех горутин

	// Проверяем, были ли ошибки
	for err := range errChan {
		if err != nil {
			return err // Возвращаем первую ошибку
		}
	}

	return nil
}

// ============================================
// РЕШЕНИЕ 2: С ожиданием готовности и таймаутом
// ============================================

func WaitForNodesReady(ctx context.Context, nodeNames []string) error {
	var wg sync.WaitGroup
	readyChan := make(chan string, len(nodeNames)) // Канал для готовых нод
	errChan := make(chan error, 1)                 // Канал для ошибок

	// Для каждой ноды запускаем горутину, которая ждет готовности
	for _, name := range nodeNames {
		wg.Add(1)
		go func(nodeName string) {
			defer wg.Done()

			// Проверяем готовность ноды в цикле
			ticker := time.NewTicker(1 * time.Second) // Проверяем каждую секунду
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done(): // Таймаут или отмена
					errChan <- fmt.Errorf("timeout waiting for node %s: %w", nodeName, ctx.Err())
					return

				case <-ticker.C: // Каждую секунду проверяем
					if IsNodeReady(nodeName) {
						fmt.Printf("🟢 Нода %s готова!\n", nodeName)
						readyChan <- nodeName
						return
					}
					// fmt.Printf("⏳ Нода %s еще не готова, ждем...\n", nodeName)
				}
			}
		}(name)
	}

	// Горутина для закрытия каналов после завершения всех проверок
	go func() {
		wg.Wait()
		close(readyChan)
		close(errChan)
	}()

	// Ждем результаты
	readyCount := 0
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout: только %d из %d нод готовы", readyCount, len(nodeNames))

		case _, ok := <-readyChan:
			if !ok { // Канал закрыт, все горутины завершены
				if readyCount == len(nodeNames) {
					fmt.Printf("✅ Все %d нод готовы!\n", readyCount)
					return nil
				}
				return fmt.Errorf("не все ноды готовы: %d из %d", readyCount, len(nodeNames))
			}
			readyCount++
			// fmt.Printf("📊 Прогресс: %d/%d нод готовы\n", readyCount, len(nodeNames))

		case err := <-errChan:
			if err != nil {
				return err
			}
		}
	}
}

// ============================================
// РЕШЕНИЕ 3: Полное решение - добавление + ожидание
// ============================================

func DeployNodesWithTimeout(nodeNames []string, timeout time.Duration) error {
	fmt.Printf("🚀 Начинаем развертывание %d нод...\n", len(nodeNames))

	// Шаг 1: Добавляем все ноды параллельно
	fmt.Println("\n=== Шаг 1: Добавление нод ===")
	if err := AddNodesParallel(nodeNames); err != nil {
		return fmt.Errorf("ошибка при добавлении нод: %w", err)
	}

	// Шаг 2: Ждем готовности всех нод с таймаутом
	fmt.Println("\n=== Шаг 2: Ожидание готовности нод ===")
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel() // Важно! Освобождаем ресурсы

	if err := WaitForNodesReady(ctx, nodeNames); err != nil {
		return fmt.Errorf("ошибка при ожидании готовности: %w", err)
	}

	fmt.Println("\n🎉 Развертывание успешно завершено!")
	return nil
}

// ============================================
// РЕШЕНИЕ 4: С отслеживанием состояния через mutex
// ============================================

type NodeTracker struct {
	mu           sync.Mutex
	nodes        map[string]bool // имя -> готовность
	totalNodes   int
	readyNodes   int
	progressChan chan string
}

func NewNodeTracker(nodeNames []string) *NodeTracker {
	tracker := &NodeTracker{
		nodes:        make(map[string]bool),
		totalNodes:   len(nodeNames),
		readyNodes:   0,
		progressChan: make(chan string, len(nodeNames)),
	}
	for _, name := range nodeNames {
		tracker.nodes[name] = false
	}
	return tracker
}

func (nt *NodeTracker) MarkReady(nodeName string) {
	nt.mu.Lock()
	defer nt.mu.Unlock()

	if !nt.nodes[nodeName] { // Проверяем, не была ли нода уже отмечена
		nt.nodes[nodeName] = true
		nt.readyNodes++
		nt.progressChan <- nodeName
	}
}

func (nt *NodeTracker) GetProgress() (ready, total int) {
	nt.mu.Lock()
	defer nt.mu.Unlock()
	return nt.readyNodes, nt.totalNodes
}

func (nt *NodeTracker) AllReady() bool {
	nt.mu.Lock()
	defer nt.mu.Unlock()
	return nt.readyNodes == nt.totalNodes
}

func DeployWithTracker(nodeNames []string, timeout time.Duration) error {
	tracker := NewNodeTracker(nodeNames)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var wg sync.WaitGroup

	// Шаг 1: Добавляем ноды
	fmt.Println("=== Добавление нод ===")
	for _, name := range nodeNames {
		wg.Add(1)
		go func(nodeName string) {
			defer wg.Done()
			if err := AddNode(nodeName); err != nil {
				fmt.Printf("❌ Ошибка добавления %s: %v\n", nodeName, err)
			}
		}(name)
	}
	wg.Wait()

	// Шаг 2: Ждем готовности
	fmt.Println("\n=== Ожидание готовности ===")
	for _, name := range nodeNames {
		wg.Add(1)
		go func(nodeName string) {
			defer wg.Done()
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if IsNodeReady(nodeName) {
						tracker.MarkReady(nodeName)
						fmt.Printf("✅ %s готова\n", nodeName)
						return
					}
				}
			}
		}(name)
	}

	// Мониторинг прогресса
	go func() {
		wg.Wait()
		close(tracker.progressChan)
	}()

	// Ждем завершения или таймаута
	for {
		select {
		case <-ctx.Done():
			ready, total := tracker.GetProgress()
			return fmt.Errorf("timeout: %d/%d нод готовы", ready, total)
		case _, ok := <-tracker.progressChan:
			if !ok {
				if tracker.AllReady() {
					return nil
				}
				ready, total := tracker.GetProgress()
				return fmt.Errorf("не все ноды готовы: %d/%d", ready, total)
			}
			ready, total := tracker.GetProgress()
			fmt.Printf("📊 Прогресс: %d/%d\n", ready, total)
		}
	}
}

// DemoWaitGroup - демонстрационная функция для вызова из main
func DemoWaitGroup() {
	nodeNames := []string{
		"node-1", "node-2", "node-3", "node-4",
		"node-5", "node-6", "node-7", "node-8",
	}
	
	timeout := 30 * time.Second
	
	if err := DeployNodesWithTimeout(nodeNames, timeout); err != nil {
		fmt.Printf("\n❌ Ошибка: %v\n", err)
	}
}
