package errorgroup

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// ═══════════════════════════════════════════════════════════════════
// Пример 1: Базовое использование errgroup
// ═══════════════════════════════════════════════════════════════════

func DemoBasicErrgroup() {
	fmt.Println("\n=== Базовое использование errgroup ===")

	g := new(errgroup.Group)

	// Запускаем несколько горутин
	for i := 1; i <= 5; i++ {
		taskID := i
		g.Go(func() error {
			fmt.Printf("Задача %d: начало\n", taskID)
			time.Sleep(time.Duration(taskID) * 100 * time.Millisecond)

			// Задача 3 вернет ошибку
			if taskID == 3 {
				fmt.Printf("Задача %d: ОШИБКА!\n", taskID)
				return fmt.Errorf("ошибка в задаче %d", taskID)
			}

			fmt.Printf("Задача %d: завершена\n", taskID)
			return nil
		})
	}

	// Wait ждет завершения всех горутин и возвращает первую ошибку
	if err := g.Wait(); err != nil {
		fmt.Printf("❌ Получена ошибка: %v\n", err)
	} else {
		fmt.Println("✅ Все задачи выполнены без ошибок")
	}
}

// ═══════════════════════════════════════════════════════════════════
// Пример 2: errgroup с контекстом и отменой
// ═══════════════════════════════════════════════════════════════════

func DemoErrgroupWithContext() {
	fmt.Println("\n=== errgroup с контекстом ===")

	// WithContext создает группу с контекстом, который автоматически
	// отменяется когда первая горутина возвращает ошибку
	g, ctx := errgroup.WithContext(context.Background())

	for i := 1; i <= 5; i++ {
		taskID := i
		g.Go(func() error {
			for j := 0; j < 10; j++ {
				select {
				case <-ctx.Done():
					// Контекст отменен - прекращаем работу
					fmt.Printf("Задача %d: отменена на итерации %d\n", taskID, j)
					return ctx.Err()
				default:
					time.Sleep(100 * time.Millisecond)
				}

				// Задача 2 выдаст ошибку на 5-й итерации
				if taskID == 2 && j == 5 {
					fmt.Printf("Задача %d: ОШИБКА на итерации %d!\n", taskID, j)
					return fmt.Errorf("ошибка в задаче %d", taskID)
				}

				fmt.Printf("Задача %d: итерация %d\n", taskID, j)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		fmt.Printf("❌ Ошибка: %v\n", err)
		fmt.Println("Все остальные задачи были отменены через контекст")
	}
}

// ═══════════════════════════════════════════════════════════════════
// Пример 3: Ограничение количества параллельных горутин
// ═══════════════════════════════════════════════════════════════════

func DemoErrgroupWithLimit() {
	fmt.Println("\n=== errgroup с ограничением параллелизма ===")

	g := new(errgroup.Group)
	g.SetLimit(3) // Максимум 3 горутины одновременно

	tasks := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	for _, taskID := range tasks {
		id := taskID
		g.Go(func() error {
			fmt.Printf("Задача %d: начало\n", id)
			time.Sleep(500 * time.Millisecond)
			fmt.Printf("Задача %d: завершена\n", id)
			return nil
		})
	}

	fmt.Println("Запущено 10 задач, но максимум 3 выполняются одновременно")

	if err := g.Wait(); err != nil {
		fmt.Printf("❌ Ошибка: %v\n", err)
	} else {
		fmt.Println("✅ Все задачи выполнены")
	}
}

// ═══════════════════════════════════════════════════════════════════
// Пример 4: Сравнение errgroup vs WaitGroup
// ═══════════════════════════════════════════════════════════════════

func processWithWaitGroup(ids []int) error {
	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	for _, id := range ids {
		wg.Add(1)
		taskID := id
		go func() {
			defer wg.Done()

			if taskID == 5 {
				select {
				case errCh <- fmt.Errorf("ошибка в задаче %d", taskID):
				default:
				}
				return
			}

			time.Sleep(100 * time.Millisecond)
		}()
	}

	wg.Wait()
	close(errCh)

	return <-errCh
}

func processWithErrgroup(ids []int) error {
	g := new(errgroup.Group)

	for _, id := range ids {
		taskID := id
		g.Go(func() error {
			if taskID == 5 {
				return fmt.Errorf("ошибка в задаче %d", taskID)
			}

			time.Sleep(100 * time.Millisecond)
			return nil
		})
	}

	return g.Wait()
}

func DemoComparison() {
	fmt.Println("\n=== Сравнение WaitGroup vs errgroup ===")

	ids := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	fmt.Println("\n1. С WaitGroup (нужен канал для ошибок):")
	if err := processWithWaitGroup(ids); err != nil {
		fmt.Printf("   ❌ Ошибка: %v\n", err)
	}

	fmt.Println("\n2. С errgroup (проще и чище):")
	if err := processWithErrgroup(ids); err != nil {
		fmt.Printf("   ❌ Ошибка: %v\n", err)
	}

	fmt.Println("\n✅ errgroup автоматически собирает ошибки!")
}

// ═══════════════════════════════════════════════════════════════════
// Пример 5: Практический пример - параллельная загрузка данных
// ═══════════════════════════════════════════════════════════════════

type DataItem struct {
	ID   int
	Data string
}

func fetchData(id int) (DataItem, error) {
	time.Sleep(200 * time.Millisecond)

	// Имитация ошибки для некоторых ID
	if id%7 == 0 {
		return DataItem{}, fmt.Errorf("не удалось загрузить данные для ID %d", id)
	}

	return DataItem{
		ID:   id,
		Data: fmt.Sprintf("Данные #%d", id),
	}, nil
}

func DemoDataFetching() {
	fmt.Println("\n=== Параллельная загрузка данных ===")

	g, ctx := errgroup.WithContext(context.Background())

	results := make(chan DataItem, 10)
	ids := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	// Загружаем данные параллельно
	for _, id := range ids {
		itemID := id
		g.Go(func() error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			fmt.Printf("Загрузка ID %d...\n", itemID)
			item, err := fetchData(itemID)
			if err != nil {
				fmt.Printf("❌ Ошибка загрузки ID %d: %v\n", itemID, err)
				return err
			}

			results <- item
			fmt.Printf("✅ Загружен ID %d\n", itemID)
			return nil
		})
	}

	// Горутина для закрытия канала результатов
	go func() {
		g.Wait()
		close(results)
	}()

	// Собираем результаты
	loadedItems := []DataItem{}
	for item := range results {
		loadedItems = append(loadedItems, item)
	}

	// Проверяем ошибки
	if err := g.Wait(); err != nil {
		fmt.Printf("\n❌ Загрузка завершена с ошибкой: %v\n", err)
		fmt.Printf("Успешно загружено: %d из %d элементов\n", len(loadedItems), len(ids))
	} else {
		fmt.Printf("\n✅ Все данные загружены: %d элементов\n", len(loadedItems))
	}
}

// ═══════════════════════════════════════════════════════════════════
// Запуск всех примеров
// ═══════════════════════════════════════════════════════════════════

func RunErrGroupExamples() {
	fmt.Println("\n╔═══════════════════════════════════════════╗")
	fmt.Println("║         ERRGROUP - ОБРАБОТКА ОШИБОК      ║")
	fmt.Println("╚═══════════════════════════════════════════╝")

	DemoBasicErrgroup()
	DemoErrgroupWithContext()
	DemoErrgroupWithLimit()
	DemoComparison()
	DemoDataFetching()

	fmt.Println("\n╔═══════════════════════════════════════════╗")
	fmt.Println("║  ИТОГ: errgroup vs WaitGroup              ║")
	fmt.Println("╠═══════════════════════════════════════════╣")
	fmt.Println("║ ✅ errgroup: автоматический сбор ошибок   ║")
	fmt.Println("║ ✅ Интеграция с контекстом                ║")
	fmt.Println("║ ✅ Ограничение параллелизма (SetLimit)    ║")
	fmt.Println("║ ✅ Меньше boilerplate кода                ║")
	fmt.Println("╚═══════════════════════════════════════════╝")
}
