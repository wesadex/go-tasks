package cache

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrNotFound = errors.New("element not found")
var ErrElementExpired = errors.New("element has expired")

type ICache interface {
	Get(string) (string, error)
	Set(string, string) error
	Del(string) error
}

type element struct {
	value      string
	ExpiryDate time.Time
}

type Cache struct {
	storage  map[string]element
	mu       *sync.Mutex
	done     chan struct{}
	wg       sync.WaitGroup
	stopOnce sync.Once
	TTL      time.Duration
}

func New(ttl time.Duration) (*Cache, func()) { // returns cache and stop function
	cache := &Cache{
		storage: make(map[string]element),
		mu:      &sync.Mutex{},
		TTL:     ttl,
		done:    make(chan struct{}),
	}
	cache.clearByTTL()
	return cache, cache.stop
}

func (c *Cache) Get(_ context.Context, key string) (string, error) {
	c.mu.Lock()
	val, ok := c.storage[key]
	c.mu.Unlock()

	if !ok {
		return "", ErrNotFound
	}

	if val.ExpiryDate.Before(time.Now()) {
		c.delete(key)
		return "", ErrElementExpired
	}

	return val.value, nil
}

func (c *Cache) clearByTTL() {
	ticker := time.NewTicker(10 * time.Second)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				c.clear()
			case <-c.done:
				return
			}
		}
	}()
}

func (c *Cache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, val := range c.storage {
		if val.ExpiryDate.Before(time.Now()) {
			delete(c.storage, key)
		}
	}
}

func (c *Cache) stop() {
	c.stopOnce.Do(func() {
		close(c.done) // Сигнализируем горутине о завершении
		c.wg.Wait()   // Ждем завершения фоновой горутины
		// Не присваиваем nil - GC сам очистит память
	})
}

func (c *Cache) Set(_ context.Context, key, value string) error {
	c.mu.Lock()
	c.storage[key] = element{
		value:      value,
		ExpiryDate: time.Now().Add(c.TTL),
	}
	c.mu.Unlock()
	return nil
}

func (c *Cache) delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.storage, key)
}

func (c *Cache) Del(_ context.Context, key string) error {
	c.delete(key)
	return nil
}

// ═══════════════════════════════════════════════════════════════════
// Примеры использования асинхронного кеша
// ═══════════════════════════════════════════════════════════════════

func RunCacheExamples() {
	println("\n╔═══════════════════════════════════════════╗")
	println("║       АСИНХРОННЫЙ КЕШ С TTL               ║")
	println("╚═══════════════════════════════════════════╝")

	// Пример 1: Базовое использование
	DemoBasicUsage()

	// Пример 2: Истечение срока действия (TTL)
	DemoExpiration()

	// Пример 3: Конкурентный доступ
	DemoConcurrentAccess()

	// Пример 4: Работа с контекстом
	DemoContextUsage()

	println("\n╔═══════════════════════════════════════════╗")
	println("║  ИТОГ: Асинхронный кеш                    ║")
	println("╠═══════════════════════════════════════════╣")
	println("║ 1. Set/Get/Del операции                  ║")
	println("║ 2. TTL и автоматическое истечение         ║")
	println("║ 3. Thread-safe с sync.Mutex               ║")
	println("║ 4. Поддержка context.Context              ║")
	println("╚═══════════════════════════════════════════╝")
}

// DemoBasicUsage демонстрирует базовые операции с кешем
func DemoBasicUsage() {
	println("\n=== Пример 1: Базовое использование ===")
	println("Создаем кеш с TTL = 5 секунд\n")

	cache, stop := New(5 * time.Second)
	defer stop()
	ctx := context.Background()

	// Set
	println("📝 Сохраняем данные в кеш...")
	_ = cache.Set(ctx, "user:1", "Alice")
	_ = cache.Set(ctx, "user:2", "Bob")
	_ = cache.Set(ctx, "product:100", "Laptop")

	// Get
	println("📖 Читаем данные из кеша...")
	if val, err := cache.Get(ctx, "user:1"); err == nil {
		println("   ✅ user:1 =", val)
	}
	if val, err := cache.Get(ctx, "user:2"); err == nil {
		println("   ✅ user:2 =", val)
	}
	if val, err := cache.Get(ctx, "product:100"); err == nil {
		println("   ✅ product:100 =", val)
	}

	// Del
	println("\n🗑️  Удаляем user:2 из кеша...")
	_ = cache.Del(ctx, "user:2")

	// Проверяем удаление
	if _, err := cache.Get(ctx, "user:2"); err != nil {
		println("   ✅ user:2 удален:", err.Error())
	}

	// Попытка получить несуществующий ключ
	if _, err := cache.Get(ctx, "user:999"); err != nil {
		println("   ✅ user:999 не найден:", err.Error())
	}
}

// DemoExpiration демонстрирует истечение срока действия элементов
func DemoExpiration() {
	println("\n=== Пример 2: Истечение срока действия (TTL) ===")
	println("Создаем кеш с TTL = 2 секунды\n")

	cache, stop := New(2 * time.Second)
	defer stop()
	ctx := context.Background()

	// Сохраняем данные
	println("📝 Сохраняем данные с TTL = 2s...")
	_ = cache.Set(ctx, "session:abc123", "active")
	_ = cache.Set(ctx, "temp:data", "temporary value")

	// Читаем сразу
	println("\n📖 Читаем сразу после сохранения:")
	if val, err := cache.Get(ctx, "session:abc123"); err == nil {
		println("   ✅ session:abc123 =", val)
	}

	// Ждем 1 секунду
	println("\n⏱️  Ждем 1 секунду (половина TTL)...")
	time.Sleep(1 * time.Second)

	println("📖 Читаем снова (данные еще актуальны):")
	if val, err := cache.Get(ctx, "session:abc123"); err == nil {
		println("   ✅ session:abc123 =", val)
	}

	// Ждем еще 1.5 секунды (всего 2.5 секунды)
	println("\n⏱️  Ждем еще 1.5 секунды (TTL истек)...")
	time.Sleep(1500 * time.Millisecond)

	println("📖 Пытаемся прочитать (TTL истек):")
	if _, err := cache.Get(ctx, "session:abc123"); err != nil {
		println("   ❌ session:abc123:", err.Error())
	}
	if _, err := cache.Get(ctx, "temp:data"); err != nil {
		println("   ❌ temp:data:", err.Error())
	}
}

// DemoConcurrentAccess демонстрирует безопасный конкурентный доступ
func DemoConcurrentAccess() {
	println("\n=== Пример 3: Конкурентный доступ ===")
	println("10 горутин одновременно работают с кешем\n")

	cache, stop := New(10 * time.Second)
	defer stop()
	ctx := context.Background()

	var wg sync.WaitGroup

	// 5 горутин пишут
	println("✍️  Запускаем 5 горутин для записи...")
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := "key:" + string(rune('0'+id))
			value := "value-" + string(rune('0'+id))
			_ = cache.Set(ctx, key, value)
			println("   [Writer", id, "] Set", key, "=", value)
		}(i)
	}

	// Даем время для записи
	time.Sleep(100 * time.Millisecond)

	// 5 горутин читают
	println("\n📖 Запускаем 5 горутин для чтения...")
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := "key:" + string(rune('0'+id))
			if val, err := cache.Get(ctx, key); err == nil {
				println("   [Reader", id, "] Get", key, "=", val)
			}
		}(i)
	}

	wg.Wait()
	println("\n✅ Все горутины завершены (без race condition!)")
}

// DemoContextUsage демонстрирует работу с контекстом
func DemoContextUsage() {
	println("\n=== Пример 4: Работа с контекстом ===")
	println("Используем context с timeout\n")

	cache, stop := New(10 * time.Second)
	defer stop()

	// Контекст с таймаутом
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	println("📝 Сохраняем данные с контекстом...")
	_ = cache.Set(ctx, "api:response", `{"status": "ok"}`)

	println("📖 Читаем данные с контекстом...")
	if val, err := cache.Get(ctx, "api:response"); err == nil {
		println("   ✅ api:response =", val)
	}

	// Проверяем, что контекст еще активен
	select {
	case <-ctx.Done():
		println("   ❌ Контекст отменен:", ctx.Err())
	default:
		println("   ✅ Контекст активен, операции выполнены успешно")
	}

	println("\n💡 Примечание: Кеш готов к интеграции с context.WithCancel")
	println("   для отмены длительных операций")
}
