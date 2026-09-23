package http

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// ═══════════════════════════════════════════════════════════════════
// Пример 1: Базовый - параллельные HTTP запросы с подсчетом статус-кодов
// ═══════════════════════════════════════════════════════════════════

// ProcessURLs реализует параллельные запросы по адресам из списка
// Подсчитывает количество для каждого StatusCode ответа
// Предусматривает возможность отмены запроса по таймауту
func ProcessURLs(urls []string) {
	statusCodeCounts := make(map[int]int)
	mu := sync.Mutex{} // Защищаем map от race condition
	wg := sync.WaitGroup{}
	client := &http.Client{}

	wg.Add(len(urls))
	for _, url := range urls {
		url := url // Захват переменной
		go func() {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				fmt.Printf("❌ Ошибка создания запроса для %s: %v\n", url, err)
				return
			}

			resp, err := client.Do(req)
			if err != nil {
				fmt.Printf("❌ Ошибка запроса к %s: %v\n", url, err)
				mu.Lock()
				statusCodeCounts[0]++ // 0 для ошибок
				mu.Unlock()
				return
			}
			defer resp.Body.Close()

			mu.Lock()
			statusCodeCounts[resp.StatusCode]++
			mu.Unlock()

			fmt.Printf("✅ %s → %d %s\n", url, resp.StatusCode, http.StatusText(resp.StatusCode))
		}()
	}

	wg.Wait()

	// Выводим статистику
	fmt.Println("\n📊 Статистика статус-кодов:")
	for code, count := range statusCodeCounts {
		if code == 0 {
			fmt.Printf("   ❌ Ошибки: %d\n", count)
		} else {
			fmt.Printf("   %d (%s): %d\n", code, http.StatusText(code), count)
		}
	}
}

// ═══════════════════════════════════════════════════════════════════
// Пример 2: С ограничением параллелизма (семафор)
// ═══════════════════════════════════════════════════════════════════

func ProcessURLsWithLimit(urls []string, maxConcurrent int) {
	statusCodeCounts := make(map[int]int)
	mu := sync.Mutex{}
	wg := sync.WaitGroup{}
	client := &http.Client{Timeout: 3 * time.Second}

	// Семафор для ограничения количества параллельных запросов
	sem := make(chan struct{}, maxConcurrent)

	wg.Add(len(urls))
	for _, url := range urls {
		url := url
		go func() {
			defer wg.Done()

			// Захватываем слот
			sem <- struct{}{}
			defer func() { <-sem }() // Освобождаем слот

			req, err := http.NewRequest(http.MethodGet, url, nil)
			if err != nil {
				fmt.Printf("❌ Ошибка создания запроса для %s: %v\n", url, err)
				return
			}

			resp, err := client.Do(req)
			if err != nil {
				fmt.Printf("❌ Ошибка запроса к %s: %v\n", url, err)
				mu.Lock()
				statusCodeCounts[0]++
				mu.Unlock()
				return
			}
			defer resp.Body.Close()

			mu.Lock()
			statusCodeCounts[resp.StatusCode]++
			mu.Unlock()

			fmt.Printf("✅ [%d/%d] %s → %d\n", len(sem), maxConcurrent, url, resp.StatusCode)
		}()
	}

	wg.Wait()

	fmt.Println("\n📊 Статистика (с ограничением параллелизма):")
	for code, count := range statusCodeCounts {
		if code == 0 {
			fmt.Printf("   ❌ Ошибки: %d\n", count)
		} else {
			fmt.Printf("   %d: %d запросов\n", code, count)
		}
	}
}

// ═══════════════════════════════════════════════════════════════════
// Пример 3: С каналом результатов
// ═══════════════════════════════════════════════════════════════════

type RequestResult struct {
	URL        string
	StatusCode int
	Duration   time.Duration
	Error      error
}

func ProcessURLsWithResults(urls []string) []RequestResult {
	results := make(chan RequestResult, len(urls))
	wg := sync.WaitGroup{}
	client := &http.Client{Timeout: 3 * time.Second}

	wg.Add(len(urls))
	for _, url := range urls {
		url := url
		go func() {
			defer wg.Done()

			startTime := time.Now()

			req, err := http.NewRequest(http.MethodGet, url, nil)
			if err != nil {
				results <- RequestResult{
					URL:      url,
					Duration: time.Since(startTime),
					Error:    err,
				}
				return
			}

			resp, err := client.Do(req)
			duration := time.Since(startTime)

			if err != nil {
				results <- RequestResult{
					URL:      url,
					Duration: duration,
					Error:    err,
				}
				return
			}
			defer resp.Body.Close()

			results <- RequestResult{
				URL:        url,
				StatusCode: resp.StatusCode,
				Duration:   duration,
				Error:      nil,
			}
		}()
	}

	// Закрываем канал после завершения всех горутин
	go func() {
		wg.Wait()
		close(results)
	}()

	// Собираем результаты
	var allResults []RequestResult
	for result := range results {
		allResults = append(allResults, result)

		if result.Error != nil {
			fmt.Printf("❌ %s → Ошибка: %v (%v)\n", result.URL, result.Error, result.Duration)
		} else {
			fmt.Printf("✅ %s → %d (%v)\n", result.URL, result.StatusCode, result.Duration)
		}
	}

	return allResults
}

// ═══════════════════════════════════════════════════════════════════
// Пример 4: Worker Pool с каналом URL и ограничением воркеров
// ═══════════════════════════════════════════════════════════════════

func ProcessURLsWithWorkerPool(urls []string, maxConnects int) map[int]int {
	statusCodeCounts := make(map[int]int)
	mu := sync.Mutex{}
	wg := sync.WaitGroup{}
	client := &http.Client{}

	// Канал для передачи URL между воркерами
	ch := make(chan string)

	// Заполняем канал URL-ами
	go func() {
		for _, url := range urls {
			ch <- url
		}
		close(ch)
	}()

	// Функция для обработки одного URL
	// Вынесена отдельно, чтобы defer cancel() корректно отменял context после каждого запроса
	processURL := func(url string) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel() // ✅ Теперь cancel вызывается после каждого URL, а не в конце горутины

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			fmt.Printf("❌ Ошибка создания запроса для %s: %v\n", url, err)
			return
		}

		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("❌ Ошибка запроса к %s: %v\n", url, err)
			mu.Lock()
			statusCodeCounts[0]++
			mu.Unlock()
			return
		}
		defer resp.Body.Close()

		mu.Lock()
		statusCodeCounts[resp.StatusCode]++
		mu.Unlock()

		fmt.Printf("✅ %s → %d %s\n", url, resp.StatusCode, http.StatusText(resp.StatusCode))
	}

	// Запускаем ограниченное количество воркеров
	wg.Add(maxConnects)
	for range maxConnects {
		go func() {
			defer wg.Done()

			// Каждый воркер берет URL из канала и обрабатывает его
			for url := range ch {
				processURL(url)
			}
		}()
	}

	wg.Wait()

	return statusCodeCounts
}

// ═══════════════════════════════════════════════════════════════════
// Демонстрация всех вариантов
// ═══════════════════════════════════════════════════════════════════

func RunHTTPExamples() {
	fmt.Println("\n╔═══════════════════════════════════════════╗")
	fmt.Println("║    ПАРАЛЛЕЛЬНЫЕ HTTP ЗАПРОСЫ              ║")
	fmt.Println("╚═══════════════════════════════════════════╝")

	// Тестовые URL (публичные API)
	urls := []string{
		"https://httpbin.org/status/200",
		"https://httpbin.org/status/201",
		"https://httpbin.org/status/404",
		"https://httpbin.org/delay/1",
		"https://jsonplaceholder.typicode.com/posts/1",
		"https://jsonplaceholder.typicode.com/users/1",
		"https://api.github.com/zen",
		"https://api.github.com/users/github",
		"https://httpbin.org/status/500",
		"https://invalid-url-that-does-not-exist-12345.com",
	}

	// Пример 1: Базовый с подсчетом статус-кодов
	fmt.Println("\n=== Пример 1: Базовый (с mutex для map) ===")
	fmt.Printf("Запускаем %d параллельных запросов...\n\n", len(urls))
	startTime := time.Now()
	ProcessURLs(urls)
	fmt.Printf("\n⏱️  Время выполнения: %v\n", time.Since(startTime))

	// Пример 2: С ограничением параллелизма
	fmt.Println("\n" + "=================================================")
	fmt.Println("=== Пример 2: С ограничением параллелизма ===")
	maxConcurrent := 3
	fmt.Printf("Максимум одновременных запросов: %d\n\n", maxConcurrent)
	startTime = time.Now()
	ProcessURLsWithLimit(urls, maxConcurrent)
	fmt.Printf("\n⏱️  Время выполнения: %v\n", time.Since(startTime))

	// Пример 3: С каналом результатов
	fmt.Println("\n" + "=================================================")
	fmt.Println("=== Пример 3: С каналом результатов ===")
	fmt.Println("(Возвращаем структурированные результаты)")
	fmt.Println()
	startTime = time.Now()
	results := ProcessURLsWithResults(urls)
	fmt.Printf("\n⏱️  Время выполнения: %v\n", time.Since(startTime))

	// Анализ результатов
	successCount := 0
	for _, r := range results {
		if r.Error == nil && r.StatusCode >= 200 && r.StatusCode < 300 {
			successCount++
		}
	}
	fmt.Printf("\n✅ Успешных запросов: %d из %d\n", successCount, len(results))

	// Пример 4: Worker Pool с каналом URL
	fmt.Println("\n" + "=================================================")
	fmt.Println("=== Пример 4: Worker Pool с каналом URL ===")
	maxWorkers := 3
	fmt.Printf("Количество воркеров: %d\n", maxWorkers)
	fmt.Println("(Воркеры берут URL из общего канала)")
	fmt.Println()
	startTime = time.Now()
	statusCounts := ProcessURLsWithWorkerPool(urls, maxWorkers)
	fmt.Printf("\n⏱️  Время выполнения: %v\n", time.Since(startTime))

	// Выводим статистику
	fmt.Println("\n📊 Статистика (Worker Pool):")
	for code, count := range statusCounts {
		if code == 0 {
			fmt.Printf("   ❌ Ошибки: %d\n", count)
		} else {
			fmt.Printf("   %d (%s): %d\n", code, http.StatusText(code), count)
		}
	}

	fmt.Println("\n╔═══════════════════════════════════════════╗")
	fmt.Println("║  ИТОГ: Паттерны HTTP запросов             ║")
	fmt.Println("╠═══════════════════════════════════════════╣")
	fmt.Println("║ 1. sync.Mutex для защиты map              ║")
	fmt.Println("║ 2. Семафор для ограничения запросов       ║")
	fmt.Println("║ 3. Канал результатов для сбора данных     ║")
	fmt.Println("║ 4. Worker Pool с каналом задач            ║")
	fmt.Println("║ 5. context.WithTimeout для таймаутов      ║")
	fmt.Println("╚═══════════════════════════════════════════╝")
}
