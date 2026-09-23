package channels

import (
	"fmt"
	"sync"
	"time"
)

// ═══════════════════════════════════════════════════════════════════
// Паттерн Fan-In: Объединение нескольких каналов в один
// ═══════════════════════════════════════════════════════════════════

// fanIn объединяет несколько входных каналов в один выходной
func fanIn(chans ...<-chan int) <-chan int {
	result := make(chan int)
	wg := sync.WaitGroup{}

	go func() {
		for _, ch := range chans {
			wg.Add(1)
			go func(c <-chan int) {
				defer wg.Done()

				for val := range c {
					result <- val
				}
			}(ch)
		}

		wg.Wait()
		close(result)
	}()

	return result
}

// DemoFanIn демонстрирует паттерн Fan-In
func DemoFanIn() {
	fmt.Println("\n=== Паттерн Fan-In ===")

	// Создаем несколько источников данных
	ch1 := make(chan int)
	ch2 := make(chan int)
	ch3 := make(chan int)

	// Генераторы данных
	go func() {
		for i := 1; i <= 3; i++ {
			ch1 <- i * 10
			time.Sleep(100 * time.Millisecond)
		}
		close(ch1)
	}()

	go func() {
		for i := 1; i <= 3; i++ {
			ch2 <- i * 100
			time.Sleep(150 * time.Millisecond)
		}
		close(ch2)
	}()

	go func() {
		for i := 1; i <= 3; i++ {
			ch3 <- i * 1000
			time.Sleep(200 * time.Millisecond)
		}
		close(ch3)
	}()

	// Объединяем все каналы в один
	merged := fanIn(ch1, ch2, ch3)

	// Читаем из объединенного канала
	fmt.Println("Получаем данные из всех каналов:")
	for val := range merged {
		fmt.Printf("  Получено: %d\n", val)
	}
	fmt.Println("Все каналы закрыты!")
}

// ═══════════════════════════════════════════════════════════════════
// Паттерн Fan-Out: Распределение работы по нескольким воркерам
// ═══════════════════════════════════════════════════════════════════

func worker(id int, jobs <-chan int, results chan<- int) {
	for job := range jobs {
		fmt.Printf("  Worker %d обрабатывает задачу %d\n", id, job)
		time.Sleep(100 * time.Millisecond)
		results <- job * 2
	}
}

func DemoFanOut() {
	fmt.Println("\n=== Паттерн Fan-Out (Worker Pool) ===")

	jobs := make(chan int, 10)
	results := make(chan int, 10)

	// Запускаем 3 воркера
	numWorkers := 3
	for w := 1; w <= numWorkers; w++ {
		go worker(w, jobs, results)
	}

	// Отправляем задачи
	numJobs := 9
	for j := 1; j <= numJobs; j++ {
		jobs <- j
	}
	close(jobs)

	// Собираем результаты
	fmt.Println("\nРезультаты:")
	for r := 1; r <= numJobs; r++ {
		result := <-results
		fmt.Printf("  Результат: %d\n", result)
	}
}

// ═══════════════════════════════════════════════════════════════════
// Паттерн Pipeline: Конвейерная обработка данных
// ═══════════════════════════════════════════════════════════════════

// Генератор чисел
func generator(nums ...int) <-chan int {
	out := make(chan int)
	go func() {
		for _, n := range nums {
			out <- n
		}
		close(out)
	}()
	return out
}

// Умножение на 2
func multiply(in <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		for n := range in {
			out <- n * 2
		}
		close(out)
	}()
	return out
}

// Прибавление 10
func add(in <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		for n := range in {
			out <- n + 10
		}
		close(out)
	}()
	return out
}

func DemoPipeline() {
	fmt.Println("\n=== Паттерн Pipeline ===")
	fmt.Println("Конвейер: generate → multiply by 2 → add 10")

	// Строим конвейер
	numbers := generator(1, 2, 3, 4, 5)
	doubled := multiply(numbers)
	result := add(doubled)

	// Читаем результаты
	fmt.Println("\nРезультаты:")
	for n := range result {
		fmt.Printf("  %d\n", n)
	}
}

// ═══════════════════════════════════════════════════════════════════
// Демонстрация всех примеров
// ═══════════════════════════════════════════════════════════════════

func RunChannelPatterns() {
	fmt.Println("\n╔═══════════════════════════════════════════╗")
	fmt.Println("║        ПАТТЕРНЫ РАБОТЫ С КАНАЛАМИ         ║")
	fmt.Println("╚═══════════════════════════════════════════╝")

	DemoFanIn()
	DemoFanOut()
	DemoPipeline()
}

