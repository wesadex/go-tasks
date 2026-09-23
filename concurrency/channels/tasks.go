package channels

import "fmt"

// ═══════════════════════════════════════════════════════════════════
// Пример 1: Рекурсивный тип канала
// ═══════════════════════════════════════════════════════════════════

type recursiveChan chan recursiveChan

// DemoRecursiveChannel демонстрирует работу с рекурсивным типом канала
func DemoRecursiveChannel() {
	fmt.Println("\n=== Рекурсивный тип канала ===")
	fmt.Println("Демонстрация канала, который передает сам себя")

	var c = make(recursiveChan, 1)
	defer close(c)
	c <- c

	for i := 0; i < 1000; i++ {
		select {
		case <-c:
			// Читаем из канала (освобождаем место)
		case <-c: // Читаем из канала (освобождаем место)
			c <- c
			// Записываем канал сам в себя
		default:
			// Чтение невозможно, канал пустой
			fmt.Printf("Итерация %d: канал заблокирован, выходим\n", i)
			return
		}
	}
	fmt.Println("Завершено 1000 итераций без блокировки")
}

// ═══════════════════════════════════════════════════════════════════
// Пример 2: Закрытие канала и проверка состояния
// ═══════════════════════════════════════════════════════════════════

func DemoChannelClose() {
	fmt.Println("\n=== Закрытие канала и проверка ===")

	ch := make(chan int, 3)

	// Отправляем данные
	ch <- 1
	ch <- 2
	ch <- 3
	close(ch) // Закрываем канал

	// Читаем до закрытия
	fmt.Println("Чтение из закрытого канала:")
	for val := range ch {
		fmt.Printf("  Получено: %d\n", val)
	}

	// Попытка чтения из закрытого канала
	val, ok := <-ch
	if !ok {
		fmt.Printf("Канал закрыт, получено нулевое значение: %d\n", val)
	}
}

// ═══════════════════════════════════════════════════════════════════
// Пример 3: Тайм-аут с использованием select
// ═══════════════════════════════════════════════════════════════════

func DemoSelectTimeout() {
	fmt.Println("\n=== Select с таймаутом ===")

	ch := make(chan string)
	timeout := make(chan bool, 1)

	// Горутина, которая НЕ отправит данные (имитация долгой работы)
	go func() {
		fmt.Println("Горутина: Работаю очень долго...")
		// Специально не отправляем, чтобы сработал таймаут
	}()

	// Имитация таймаута
	go func() {
		timeout <- true
	}()

	// Ждем с таймаутом
	select {
	case msg := <-ch:
		fmt.Printf("Получено: %s\n", msg)
	case <-timeout:
		fmt.Println("Таймаут! Данные не получены вовремя")
	}
}

// ═══════════════════════════════════════════════════════════════════
// Пример 4: Nil канал в select
// ═══════════════════════════════════════════════════════════════════

func DemoNilChannel() {
	fmt.Println("\n=== Nil канал в select ===")

	var ch1 chan int
	ch2 := make(chan int)

	go func() {
		ch2 <- 42
	}()

	select {
	case val := <-ch1:
		// Этот case НИКОГДА не выполнится, т.к. ch1 = nil
		fmt.Printf("Из ch1: %d\n", val)
	case val := <-ch2:
		fmt.Printf("Из ch2: %d (ch1 был nil и был проигнорирован)\n", val)
	}

	fmt.Println("Полезно: nil канал в select блокируется навсегда")
}

// ═══════════════════════════════════════════════════════════════════
// Пример 5: Однонаправленные каналы
// ═══════════════════════════════════════════════════════════════════

// Функция принимает только канал для чтения
func readOnly(ch <-chan int) {
	for val := range ch {
		fmt.Printf("  Прочитано: %d\n", val)
	}
}

// Функция принимает только канал для записи
func writeOnly(ch chan<- int) {
	for i := 1; i <= 3; i++ {
		ch <- i * 10
	}
	close(ch)
}

func DemoDirectionalChannels() {
	fmt.Println("\n=== Однонаправленные каналы ===")

	ch := make(chan int)

	go writeOnly(ch) // Передаем как write-only
	readOnly(ch)     // Передаем как read-only

	fmt.Println("Типобезопасность: нельзя записать в read-only канал")
}

// ═══════════════════════════════════════════════════════════════════
// Пример 6: Множественный select
// ═══════════════════════════════════════════════════════════════════

func DemoMultipleSelect() {
	fmt.Println("\n=== Множественный select ===")

	ch1 := make(chan string)
	ch2 := make(chan string)
	quit := make(chan bool)

	// Отправители
	go func() {
		ch1 <- "Канал 1"
		ch2 <- "Канал 2"
		quit <- true
	}()

	received := 0
	for received < 3 {
		select {
		case msg := <-ch1:
			fmt.Printf("Из ch1: %s\n", msg)
			received++
		case msg := <-ch2:
			fmt.Printf("Из ch2: %s\n", msg)
			received++
		case <-quit:
			fmt.Println("Получен сигнал завершения")
			received++
		}
	}
}

// ═══════════════════════════════════════════════════════════════════
// Демонстрация всех задач
// ═══════════════════════════════════════════════════════════════════

func RunChannelTasks() {
	fmt.Println("\n╔═══════════════════════════════════════════╗")
	fmt.Println("║       ДОПОЛНИТЕЛЬНЫЕ ПРИМЕРЫ КАНАЛОВ      ║")
	fmt.Println("╚═══════════════════════════════════════════╝")

	DemoRecursiveChannel()
	DemoChannelClose()
	DemoSelectTimeout()
	DemoNilChannel()
	DemoDirectionalChannels()
	DemoMultipleSelect()
}
