package concurrency

import (
	"fmt"
	"sync"
	"time"
)

// ═══════════════════════════════════════════════════════════════════
// 1. БУФЕРИЗОВАННЫЕ VS НЕБУФЕРИЗОВАННЫЕ КАНАЛЫ
// ═══════════════════════════════════════════════════════════════════

/*
КАНАЛЫ - это механизм коммуникации между горутинами

ДВА ТИПА КАНАЛОВ:
1. Небуферизованный: make(chan T)
2. Буферизованный: make(chan T, N)
*/

// ───────────────────────────────────────────────────────────────────
// 1.1 НЕБУФЕРИЗОВАННЫЙ канал (синхронный)
// ───────────────────────────────────────────────────────────────────

func DemoUnbufferedChannel() {
	fmt.Println("\n=== НЕБУФЕРИЗОВАННЫЙ КАНАЛ ===")
	ch := make(chan string) // Без буфера!

	// Отправитель
	go func() {
		fmt.Println("Горутина: Пытаюсь отправить 'Hello'...")
		ch <- "Hello" // БЛОКИРУЕТСЯ здесь пока кто-то не примет!
		fmt.Println("Горутина: Отправил! Кто-то принял.")
	}()

	time.Sleep(2 * time.Second) // Симулируем задержку
	fmt.Println("Main: Сейчас приму сообщение...")
	msg := <-ch // Получаем
	fmt.Printf("Main: Получил '%s'\n", msg)
}

/*
ВИЗУАЛИЗАЦИЯ НЕБУФЕРИЗОВАННОГО:

Горутина 1:    ch <- "data"  ┐
                              ├─ Рукопожатие (handshake)
Горутина 2:    data := <-ch  ┘

ВАЖНО:
- Отправитель БЛОКИРУЕТСЯ пока получатель не примет
- Получатель БЛОКИРУЕТСЯ пока отправитель не отправит
- Синхронная передача = прямой обмен данными
*/

// ───────────────────────────────────────────────────────────────────
// 1.2 БУФЕРИЗОВАННЫЙ канал (асинхронный)
// ───────────────────────────────────────────────────────────────────

func DemoBufferedChannel() {
	fmt.Println("\n=== БУФЕРИЗОВАННЫЙ КАНАЛ ===")
	ch := make(chan string, 2) // Буфер на 2 элемента!

	// Отправитель
	go func() {
		fmt.Println("Горутина: Отправляю 'Hello'...")
		ch <- "Hello" // НЕ блокируется! Буфер не полон
		fmt.Println("Горутина: Отправил 'Hello', буфер 1/2")

		fmt.Println("Горутина: Отправляю 'World'...")
		ch <- "World" // НЕ блокируется! Буфер не полон
		fmt.Println("Горутина: Отправил 'World', буфер 2/2 (ПОЛОН)")

		fmt.Println("Горутина: Пытаюсь отправить '!'...")
		ch <- "!" // БЛОКИРУЕТСЯ! Буфер полон!
		fmt.Println("Горутина: Отправил '!'")
	}()

	time.Sleep(2 * time.Second) // Даем горутине отправить 2 сообщения
	fmt.Println("Main: Читаю первое сообщение...")
	fmt.Printf("Main: Получил '%s'\n", <-ch) // Освобождаем место в буфере

	time.Sleep(1 * time.Second)
	fmt.Println("Main: Читаю второе сообщение...")
	fmt.Printf("Main: Получил '%s'\n", <-ch)

	time.Sleep(1 * time.Second)
	fmt.Println("Main: Читаю третье сообщение...")
	fmt.Printf("Main: Получил '%s'\n", <-ch)
}

/*
ВИЗУАЛИЗАЦИЯ БУФЕРИЗОВАННОГО:

ch := make(chan T, 3)  // Буфер на 3 элемента

┌─────────────────┐
│  Буфер [_, _, _]│  ← Пустой, можно отправить 3 без блокировки
└─────────────────┘

ch <- "A"  // НЕ блокируется
┌─────────────────┐
│  Буфер [A, _, _]│
└─────────────────┘

ch <- "B"  // НЕ блокируется
ch <- "C"  // НЕ блокируется
┌─────────────────┐
│  Буфер [A, B, C]│  ← ПОЛОН!
└─────────────────┘

ch <- "D"  // БЛОКИРУЕТСЯ! Ждет пока кто-то прочитает

<-ch       // Читаем "A", освобождаем место
┌─────────────────┐
│  Буфер [B, C, _]│  ← Теперь "D" может войти
└─────────────────┘
*/

// СРАВНЕНИЕ:
func CompareChannels() {
	fmt.Println("\n=== СРАВНЕНИЕ КАНАЛОВ ===")

	// Небуферизованный - ДЕДЛОК!
	// ch1 := make(chan int)
	// ch1 <- 42  // Блокируется навсегда - никто не читает!

	// Буферизованный - OK!
	ch2 := make(chan int, 1)
	ch2 <- 42 // НЕ блокируется, буфер не полон
	fmt.Printf("Отправил в буферизованный канал: %d\n", <-ch2)
}

/*
КОГДА ИСПОЛЬЗОВАТЬ:

НЕБУФЕРИЗОВАННЫЙ (make(chan T)):
✅ Нужна синхронизация (handshake)
✅ Гарантия что получатель принял данные
✅ Простая логика - прямая передача

БУФЕРИЗОВАННЫЙ (make(chan T, N)):
✅ Отправитель не должен ждать
✅ Сглаживание пиков нагрузки
✅ Producer-Consumer pattern
⚠️ N должен быть обоснован (не бесконечный!)
*/

// ═══════════════════════════════════════════════════════════════════
// 2. WAITGROUP - КАК РАБОТАЕТ
// ═══════════════════════════════════════════════════════════════════

/*
sync.WaitGroup - счетчик для ожидания завершения горутин

ТРИ МЕТОДА:
1. Add(n)  - увеличить счетчик на n
2. Done()  - уменьшить счетчик на 1
3. Wait()  - блокироваться пока счетчик != 0
*/

func DemoWaitGroupBasic() {
	fmt.Println("\n=== WAITGROUP: БАЗОВЫЙ ПРИМЕР ===")
	var wg sync.WaitGroup

	fmt.Println("Счетчик: 0")

	// Запускаем 3 горутины
	for i := 1; i <= 3; i++ {
		wg.Add(1) // Счетчик: +1
		fmt.Printf("Запускаем горутину %d, счетчик: %d\n", i, i)

		go func(id int) {
			defer wg.Done() // Счетчик: -1 при завершении

			fmt.Printf("  Горутина %d: работаю...\n", id)
			time.Sleep(time.Duration(id) * time.Second)
			fmt.Printf("  Горутина %d: завершилась\n", id)
		}(i)
	}

	fmt.Println("Main: Жду завершения всех горутин...")
	wg.Wait() // БЛОКИРУЕТСЯ пока счетчик != 0
	fmt.Println("Main: Все горутины завершены!")
}

/*
ВИЗУАЛИЗАЦИЯ WAITGROUP:

var wg sync.WaitGroup

┌───────────┐
│ counter: 0│
└───────────┘

wg.Add(1)  ──→  counter = 1
wg.Add(1)  ──→  counter = 2
wg.Add(1)  ──→  counter = 3

                ┌───────────┐
                │ counter: 3│
                └───────────┘

wg.Wait()  ──→  БЛОКИРУЕТСЯ (counter != 0)

Горутина 1 завершается:
wg.Done()  ──→  counter = 2

Горутина 2 завершается:
wg.Done()  ──→  counter = 1

Горутина 3 завершается:
wg.Done()  ──→  counter = 0  ──→  wg.Wait() РАЗБЛОКИРУЕТСЯ!
*/

// ПРАВИЛЬНЫЙ паттерн
func CorrectWaitGroupPattern() {
	fmt.Println("\n=== WAITGROUP: ПРАВИЛЬНЫЙ ПАТТЕРН ===")
	var wg sync.WaitGroup

	tasks := []string{"task-1", "task-2", "task-3"}

	for _, task := range tasks {
		wg.Add(1) // ДО запуска горутины!

		go func(taskName string) {
			defer wg.Done() // defer гарантирует вызов даже при панике

			fmt.Printf("Выполняю %s\n", taskName)
			time.Sleep(500 * time.Millisecond)
		}(task) // Передаем как параметр!
	}

	wg.Wait()
	fmt.Println("Все задачи выполнены!")
}

// НЕПРАВИЛЬНЫЕ паттерны
func WrongPatterns() {
	fmt.Println("\n=== WAITGROUP: ЧАСТЫЕ ОШИБКИ ===")

	// ❌ ОШИБКА 1: Add ВНУТРИ горутины
	fmt.Println("\n❌ Ошибка 1: wg.Add() внутри горутины")
	fmt.Println("   Проблема: wg.Wait() может выполниться ДО wg.Add()")

	// var wg sync.WaitGroup
	// for i := 0; i < 3; i++ {
	// 	go func() {
	// 		wg.Add(1)  // ПЛОХО! Может не успеть
	// 		defer wg.Done()
	// 	}()
	// }
	// wg.Wait()  // Может разблокироваться сразу если счетчик все еще 0!

	// ✅ ПРАВИЛЬНО:
	// wg.Add(1) ДО go func()

	// ❌ ОШИБКА 2: Забыть defer
	fmt.Println("\n❌ Ошибка 2: Забыть defer wg.Done()")
	fmt.Println("   Проблема: Если паника - Done() не вызовется")

	// ✅ ПРАВИЛЬНО: defer wg.Done()

	// ❌ ОШИБКА 3: Использовать переменную цикла
	fmt.Println("\n❌ Ошибка 3: Захват переменной цикла")
	// for _, name := range names {
	// 	go func() {
	// 		fmt.Println(name)  // ПЛОХО! Все горутины видят последний name
	// 	}()
	// }

	// ✅ ПРАВИЛЬНО: Передавать как параметр
	// go func(n string) { fmt.Println(n) }(name)
}

// ═══════════════════════════════════════════════════════════════════
// 3. SELECT - КАК РАБОТАЕТ
// ═══════════════════════════════════════════════════════════════════

/*
select - это switch для каналов

СИНТАКСИС:
select {
case val := <-ch1:
    // Если ch1 готов к чтению
case ch2 <- val:
    // Если ch2 готов к записи
case <-time.After(1 * time.Second):
    // Таймаут
default:
    // Если ничего не готово (non-blocking)
}

КАК РАБОТАЕТ:
1. Проверяет ВСЕ case одновременно
2. Если несколько готовы - выбирает СЛУЧАЙНЫЙ
3. Если ничего не готово - блокируется (или default)
4. Выполняет ТОЛЬКО ОДИН case за раз
*/

func DemoSelectBasic() {
	fmt.Println("\n=== SELECT: БАЗОВЫЙ ПРИМЕР ===")

	ch1 := make(chan string)
	ch2 := make(chan string)

	// Горутина 1
	go func() {
		time.Sleep(1 * time.Second)
		ch1 <- "от ch1"
	}()

	// Горутина 2
	go func() {
		time.Sleep(2 * time.Second)
		ch2 <- "от ch2"
	}()

	// Ждем первое сообщение
	select {
	case msg1 := <-ch1:
		fmt.Printf("Получил: %s\n", msg1)
	case msg2 := <-ch2:
		fmt.Printf("Получил: %s\n", msg2)
	}
	// Выполнится case для ch1 (он быстрее)
}

// SELECT с таймаутом
func DemoSelectTimeout() {
	fmt.Println("\n=== SELECT: ТАЙМАУТ ===")

	ch := make(chan string)

	go func() {
		time.Sleep(3 * time.Second)
		ch <- "данные"
	}()

	select {
	case msg := <-ch:
		fmt.Printf("Получил: %s\n", msg)
	case <-time.After(1 * time.Second):
		fmt.Println("Таймаут! Не дождались данных")
	}
	// Выполнится таймаут (1 секунда < 3 секунды)
}

// SELECT с default (non-blocking)
func DemoSelectDefault() {
	fmt.Println("\n=== SELECT: DEFAULT (NON-BLOCKING) ===")

	ch := make(chan string)

	// Пытаемся прочитать, но канал пуст
	select {
	case msg := <-ch:
		fmt.Printf("Получил: %s\n", msg)
	default:
		fmt.Println("Канал пуст, продолжаем работу")
	}
	// Выполнится default (канал не готов)
}

// SELECT в цикле (типичный паттерн)
func DemoSelectLoop() {
	fmt.Println("\n=== SELECT: В ЦИКЛЕ ===")

	dataCh := make(chan int)
	stopCh := make(chan bool)

	// Producer
	go func() {
		for i := 1; i <= 5; i++ {
			dataCh <- i
			time.Sleep(500 * time.Millisecond)
		}
		stopCh <- true
	}()

	// Consumer с select
	for {
		select {
		case data := <-dataCh:
			fmt.Printf("Обработал: %d\n", data)
		case <-stopCh:
			fmt.Println("Получен сигнал остановки")
			return
		}
	}
}

/*
ВИЗУАЛИЗАЦИЯ SELECT:

select {
case val := <-ch1:  ──┐
    ...               │
case val := <-ch2:  ──┼──  Проверяет ВСЕ case ОДНОВРЕМЕННО
    ...               │
case <-timeout:     ──┤
    ...               │
default:            ──┘
    ...
}

СЦЕНАРИИ:

1. Один case готов:
   ch1: ✅ готов
   ch2: ❌ не готов
   → Выполнится case ch1

2. Несколько case готовы:
   ch1: ✅ готов
   ch2: ✅ готов
   → Выбирается СЛУЧАЙНЫЙ! (равномерное распределение)

3. Ничего не готово + default:
   ch1: ❌
   ch2: ❌
   → Выполнится default (НЕ блокируется)

4. Ничего не готово + НЕТ default:
   → БЛОКИРУЕТСЯ пока что-то не станет готово
*/

// ПРАКТИЧЕСКИЙ ПРИМЕР: Worker pool с select
func WorkerPoolWithSelect() {
	fmt.Println("\n=== SELECT: WORKER POOL ===")

	jobs := make(chan int, 5)
	results := make(chan int, 5)
	done := make(chan bool)

	// Worker
	go func() {
		for {
			select {
			case job := <-jobs:
				fmt.Printf("Worker обрабатывает задачу %d\n", job)
				results <- job * 2
			case <-done:
				fmt.Println("Worker завершает работу")
				return
			}
		}
	}()

	// Отправляем задачи
	for i := 1; i <= 3; i++ {
		jobs <- i
	}

	// Получаем результаты
	for i := 1; i <= 3; i++ {
		result := <-results
		fmt.Printf("Получен результат: %d\n", result)
	}

	// Останавливаем worker
	done <- true
	time.Sleep(100 * time.Millisecond)
}

/*
РЕЗЮМЕ:

1. КАНАЛЫ:
   - Небуферизованный: Синхронная передача (handshake)
   - Буферизованный: Асинхронная передача (очередь)

2. WAITGROUP:
   - Add(n) перед запуском горутин
   - defer Done() внутри горутин
   - Wait() для ожидания

3. SELECT:
   - Switch для каналов
   - Проверяет все case одновременно
   - Выбирает случайный если несколько готовы
   - default для non-blocking операций
   - timeout через time.After()
*/

// ═══════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════

func RunConcurrencyExplanation() {
	// 1. Каналы
	DemoUnbufferedChannel()
	DemoBufferedChannel()
	CompareChannels()

	// 2. WaitGroup
	DemoWaitGroupBasic()
	CorrectWaitGroupPattern()
	WrongPatterns()

	// 3. Select
	DemoSelectBasic()
	DemoSelectTimeout()
	DemoSelectDefault()
	DemoSelectLoop()
	WorkerPoolWithSelect()
}
