package mutex

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ============================================
// Пример 1: БЕЗ mutex - race condition
// ============================================

type CounterUnsafe struct {
	value int
}

func (c *CounterUnsafe) Increment() {
	c.value++ // Небезопасно!
}

func (c *CounterUnsafe) Value() int {
	return c.value
}

func DemoUnsafe() {
	counter := &CounterUnsafe{}
	var wg sync.WaitGroup

	// Запускаем 1000 горутин, каждая инкрементирует 1000 раз
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				counter.Increment()
			}
		}()
	}

	wg.Wait()
	fmt.Printf("Unsafe counter (ожидали 1000000): %d\n", counter.Value())
	// Результат будет МЕНЬШЕ 1000000 из-за race condition!
}

// ============================================
// Пример 2: С sync.Mutex - безопасно
// ============================================

type CounterSafe struct {
	mu    sync.Mutex // Защита
	value int
}

func (c *CounterSafe) Increment() {
	c.mu.Lock()   // Блокируем доступ
	c.value++     // Критическая секция
	c.mu.Unlock() // Разблокируем
}

func (c *CounterSafe) Value() int {
	c.mu.Lock()
	defer c.mu.Unlock() // defer гарантирует разблокировку
	return c.value
}

func DemoSafe() {
	counter := &CounterSafe{}
	var wg sync.WaitGroup

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				counter.Increment()
			}
		}()
	}

	wg.Wait()
	fmt.Printf("Safe counter (ожидали 1000000): %d\n", counter.Value())
	// Результат ВСЕГДА будет 1000000!
}

// ============================================
// Пример 3: sync.RWMutex - для чтения/записи
// ============================================

// RWMutex позволяет:
// - Множественные одновременные чтения (RLock)
// - Только одна запись (Lock), блокирует всё

type Cache struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewCache() *Cache {
	return &Cache{
		data: make(map[string]string),
	}
}

func (c *Cache) Get(key string) (string, bool) {
	c.mu.RLock() // Read Lock - разрешает параллельное чтение
	defer c.mu.RUnlock()
	val, ok := c.data[key]
	return val, ok
}

func (c *Cache) Set(key, value string) {
	c.mu.Lock() // Write Lock - эксклюзивный доступ
	defer c.mu.Unlock()
	c.data[key] = value
}

func DemoRWMutex() {
	cache := NewCache()
	var wg sync.WaitGroup

	// 10 писателей
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", n)
			cache.Set(key, fmt.Sprintf("value%d", n))
			fmt.Printf("Writer %d: записал %s\n", n, key)
		}(i)
	}

	// Небольшая пауза чтобы что-то записалось
	time.Sleep(10 * time.Millisecond)

	// 100 читателей (могут читать одновременно!)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", n%10)
			if val, ok := cache.Get(key); ok {
				fmt.Printf("Reader %d: прочитал %s = %s\n", n, key, val)
			}
		}(i)
	}

	wg.Wait()
}

// ============================================
// Пример 4: Deadlock - ОПАСНОСТЬ!
// ============================================

func DemoDeadlock() {
	var mu sync.Mutex

	mu.Lock()
	fmt.Println("Первая блокировка")

	// Пытаемся заблокировать снова - DEADLOCK!
	// mu.Lock() // Раскомментируй - программа зависнет!
	// fmt.Println("Это никогда не выполнится")

	mu.Unlock()
	fmt.Println("Разблокировали")
}

// ============================================
// Пример 5: Правильные паттерны использования
// ============================================

type BankAccount struct {
	mu      sync.Mutex
	balance int
}

func (b *BankAccount) Deposit(amount int) {
	b.mu.Lock()
	defer b.mu.Unlock() // defer защищает от забытого Unlock
	b.balance += amount
}

func (b *BankAccount) Withdraw(amount int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.balance >= amount {
		b.balance -= amount
		return true
	}
	return false
}

func (b *BankAccount) Balance() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.balance
}

func DemoBankAccount() {
	account := &BankAccount{balance: 1000}
	var wg sync.WaitGroup

	// 100 горутин пытаются снять деньги
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if account.Withdraw(10) {
				fmt.Printf("Горутина %d: сняла 10, баланс: %d\n", id, account.Balance())
			} else {
				fmt.Printf("Горутина %d: недостаточно средств\n", id)
			}
		}(i)
	}

	wg.Wait()
	fmt.Printf("Финальный баланс: %d\n", account.Balance())
}

// ============================================
// Пример 6: Свой Mutex через атомарные операции
// ============================================

type myMutex struct {
	locked int64
}

func (m *myMutex) Lock() {
	for {
		if atomic.CompareAndSwapInt64(&m.locked, 0, 1) {
			return
		}
	}
}

func (m *myMutex) Unlock() {
	atomic.StoreInt64(&m.locked, 0)
}

func DemoCustomMutex() {
	wg := &sync.WaitGroup{}
	mu := myMutex{}

	c := 0

	wg.Add(1000)
	for range 1000 {
		go func() {
			defer wg.Done()

			mu.Lock()
			c++
			mu.Unlock()
		}()
	}

	wg.Wait()
	fmt.Printf("Custom mutex counter: %d\n", c)
}

// ============================================
// Главная функция для запуска всех демо
// ============================================

func RunAllDemos() {
	fmt.Println("=== 1. Unsafe Counter (Race Condition) ===")
	DemoUnsafe()

	fmt.Println("\n=== 2. Safe Counter (с Mutex) ===")
	DemoSafe()

	fmt.Println("\n=== 3. RWMutex (Read/Write Lock) ===")
	DemoRWMutex()

	fmt.Println("\n=== 4. Deadlock Demo ===")
	DemoDeadlock()

	fmt.Println("\n=== 5. Bank Account (Практический пример) ===")
	DemoBankAccount()

	fmt.Println("\n=== 6. Custom Mutex (Свой mutex через atomic) ===")
	DemoCustomMutex()
}
