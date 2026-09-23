package waitgroup

import (
	"fmt"
	"sync"
	"time"
)

type MyWaitGroup struct {
	mu     sync.Mutex
	count  int
	doneCh chan struct{} // nil means "already done / nothing to wait"
}

func (wg *MyWaitGroup) Add(delta int) {
	if delta == 0 {
		return
	}

	wg.mu.Lock()
	defer wg.mu.Unlock()

	// start new generation if going 0 -> positive
	if delta > 0 && wg.count == 0 {
		// create channel only when we actually have work
		wg.doneCh = make(chan struct{})
	}

	wg.count += delta
	if wg.count < 0 {
		panic("MyWG: negative counter")
	}

	// finish current generation when count reaches 0
	if wg.count == 0 && wg.doneCh != nil {
		close(wg.doneCh) // broadcast to all waiters
		wg.doneCh = nil  // back to zero-value-like state
	}
}

func (wg *MyWaitGroup) Done() {
	wg.Add(-1)
}

func (wg *MyWaitGroup) Wait() {
	wg.mu.Lock()
	// fast-path: nothing to wait for
	if wg.count == 0 {
		wg.mu.Unlock()
		return
	}
	ch := wg.doneCh
	wg.mu.Unlock()

	<-ch
}

// ═══════════════════════════════════════════════════════════════════
// Демонстрация работы MyWaitGroup
// ═══════════════════════════════════════════════════════════════════

// DemoMyWaitGroup демонстрирует использование самодельной реализации WaitGroup
func DemoMyWaitGroup() {
	fmt.Println("\n╔═══════════════════════════════════════════╗")
	fmt.Println("║    ДЕМО: КАСТОМНАЯ РЕАЛИЗАЦИЯ WAITGROUP   ║")
	fmt.Println("╚═══════════════════════════════════════════╝")

	fmt.Println("\n=== Пример 1: Базовое использование ===")
	fmt.Println("Запускаем 5 воркеров с MyWaitGroup")

	var wg MyWaitGroup

	// Запускаем 5 воркеров
	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			fmt.Printf("⚙️  Воркер %d начал работу\n", workerID)
			time.Sleep(time.Duration(workerID*100) * time.Millisecond)
			fmt.Printf("✅ Воркер %d завершил работу\n", workerID)
		}(i)
	}

	fmt.Println("🔄 Ожидаем завершения всех воркеров...")
	wg.Wait()
	fmt.Println("✅ Все воркеры завершены!")

	// ===================================================================
	fmt.Println("\n=== Пример 2: Повторное использование (новая генерация) ===")
	fmt.Println("MyWaitGroup можно переиспользовать после Wait()")

	// Переиспользуем тот же wg для новой генерации
	for i := 1; i <= 3; i++ {
		wg.Add(1)
		go func(taskID int) {
			defer wg.Done()
			fmt.Printf("📦 Задача %d выполняется...\n", taskID)
			time.Sleep(50 * time.Millisecond)
			fmt.Printf("✅ Задача %d выполнена\n", taskID)
		}(i)
	}

	fmt.Println("🔄 Ожидаем завершения второй генерации...")
	wg.Wait()
	fmt.Println("✅ Вторая генерация завершена!")

	// ===================================================================
	fmt.Println("\n=== Пример 3: Быстрый Wait() (когда нечего ждать) ===")

	var wg2 MyWaitGroup
	fmt.Println("Вызываем Wait() на пустом MyWaitGroup (без Add)...")
	wg2.Wait() // Должен вернуться сразу
	fmt.Println("✅ Wait() вернулся мгновенно (fast-path)")

	// ===================================================================
	fmt.Println("\n=== Пример 4: Динамическое добавление задач ===")
	fmt.Println("Добавляем задачи динамически во время выполнения")

	var wg3 MyWaitGroup
	wg3.Add(1)

	go func() {
		defer wg3.Done()
		fmt.Println("🔧 Главная задача: запускаем подзадачи...")

		// Динамически добавляем подзадачи
		for i := 1; i <= 3; i++ {
			wg3.Add(1)
			go func(subID int) {
				defer wg3.Done()
				fmt.Printf("   🔹 Подзадача %d выполняется\n", subID)
				time.Sleep(100 * time.Millisecond)
				fmt.Printf("   ✅ Подзадача %d завершена\n", subID)
			}(i)
			time.Sleep(30 * time.Millisecond)
		}

		fmt.Println("🔧 Главная задача: все подзадачи запущены")
	}()

	wg3.Wait()
	fmt.Println("✅ Все задачи (включая подзадачи) завершены!")

	// ===================================================================
	fmt.Println("\n╔═══════════════════════════════════════════╗")
	fmt.Println("║  ИТОГ: Кастомная реализация WaitGroup    ║")
	fmt.Println("╠═══════════════════════════════════════════╣")
	fmt.Println("║ ✅ Поддерживает Add/Done/Wait            ║")
	fmt.Println("║ ✅ Переиспользуется после Wait()         ║")
	fmt.Println("║ ✅ Fast-path для пустого счетчика        ║")
	fmt.Println("║ ✅ Канал создается только при надобности ║")
	fmt.Println("║ ✅ Panic при негативном счетчике         ║")
	fmt.Println("╚═══════════════════════════════════════════╝")
}
