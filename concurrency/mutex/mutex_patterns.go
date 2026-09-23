package mutex

import (
	"fmt"
	"sync"
)

// ============================================
// 1. Mutex ВНУТРИ структуры (самый частый паттерн)
// ============================================
// ЧТО ЗАЩИЩАЕТ: Поле c.value от одновременного изменения
// ПОЧЕМУ НУЖЕН: Множество горутин могут одновременно вызывать Inc()
// ПРАВИЛО: Всегда блокируй mutex ПЕРЕД чтением/записью защищаемых данных

type Counter struct {
	mu    sync.Mutex // Защищает value
	value int        // Защищаемые данные
}

func (c *Counter) Inc() {
	c.mu.Lock() // БЛОКИРУЕМ: начинаем работу с c.value
	defer c.mu.Unlock()
	c.value++ // Критическая секция: чтение + изменение + запись
}

// ============================================
// 2. Mutex КАК ОТДЕЛЬНАЯ переменная
// ============================================
// ЧТО ЗАЩИЩАЕТ: Переменную counter
// КОГДА ИСПОЛЬЗОВАТЬ: Когда mutex не часть структуры, а локальная защита
// ВАЖНО: mu и counter должны иметь одинаковую область видимости

func DemoSeparateMutex() {
	var mu sync.Mutex // Mutex находится рядом с защищаемой переменной
	counter := 0      // Защищаемая переменная

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mu.Lock()   // БЛОКИРУЕМ: перед доступом к counter
			counter++   // Критическая секция
			mu.Unlock() // РАЗБЛОКИРУЕМ: сразу после работы с counter
		}()
	}
	wg.Wait()
	fmt.Printf("Separate mutex: counter = %d\n", counter)
}

// ============================================
// 3. Package-level mutex (глобальная защита)
// ============================================
// ЧТО ЗАЩИЩАЕТ: Глобальную map globalData
// ПОЧЕМУ НУЖЕН: map НЕ безопасна для конкурентного доступа (race condition)
// ВАЖНО: Lock нужен даже для ЧТЕНИЯ map, если где-то происходит запись

var (
	globalMutex sync.Mutex             // Защищает globalData
	globalData  = make(map[string]int) // НЕ thread-safe без mutex!
)

func SetGlobal(key string, value int) {
	globalMutex.Lock() // БЛОКИРУЕМ: перед записью в map
	defer globalMutex.Unlock()
	globalData[key] = value // Критическая секция: запись в map
}

func GetGlobal(key string) int {
	globalMutex.Lock() // БЛОКИРУЕМ: даже для чтения!
	defer globalMutex.Unlock()
	return globalData[key] // Критическая секция: чтение из map
}

// ============================================
// 4. Mutex защищает НЕСКОЛЬКО переменных
// ============================================
// ЧТО ЗАЩИЩАЕТ: Все три переменные x, y, z одновременно
// КОГДА ИСПОЛЬЗОВАТЬ: Когда переменные логически связаны
// ПРЕИМУЩЕСТВО: Гарантирует атомарность всей операции

func DemoMultipleVariables() {
	var mu sync.Mutex // Один mutex защищает несколько переменных
	x := 0
	y := 0
	z := 0

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mu.Lock() // БЛОКИРУЕМ: перед изменением ЛЮБОЙ из трех переменных
			// Критическая секция: изменяем все три атомарно
			x++
			y += 2
			z += 3
			mu.Unlock() // РАЗБЛОКИРУЕМ: после изменения ВСЕХ переменных
		}()
	}
	wg.Wait()
	fmt.Printf("Multiple vars: x=%d, y=%d, z=%d\n", x, y, z)
}

// ============================================
// 5. Несколько mutex в одной структуре
// ============================================
// ЧТО ЗАЩИЩАЕТ: muReads защищает reads, muWrites защищает writes
// КОГДА ИСПОЛЬЗОВАТЬ: Когда данные независимы и можно параллелить
// ПРЕИМУЩЕСТВО: Больше параллелизма - горутины не блокируют друг друга

type ComplexData struct {
	// Разные mutex для НЕЗАВИСИМЫХ частей данных
	muReads  sync.Mutex // Защищает только reads
	muWrites sync.Mutex // Защищает только writes

	reads  int // Защищается muReads
	writes int // Защищается muWrites
}

func (c *ComplexData) IncrementReads() {
	c.muReads.Lock() // БЛОКИРУЕМ: только muReads
	defer c.muReads.Unlock()
	c.reads++ // Критическая секция: изменяем reads
	// muWrites НЕ блокируется! Другие горутины могут работать с writes
}

func (c *ComplexData) IncrementWrites() {
	c.muWrites.Lock() // БЛОКИРУЕМ: только muWrites
	defer c.muWrites.Unlock()
	c.writes++ // Критическая секция: изменяем writes
	// muReads НЕ блокируется! Другие горутины могут работать с reads
}

// ============================================
// 6. Mutex с closure (замыкание)
// ============================================
// ЧТО ЗАЩИЩАЕТ: Переменную counter, захваченную в замыкании
// КОГДА ИСПОЛЬЗОВАТЬ: Для инкапсуляции - снаружи невозможно забыть mutex
// ПРЕИМУЩЕСТВО: Безопасность - mutex и данные скрыты вместе

func CreateProtectedCounter() func() int {
	var mu sync.Mutex // Mutex захватывается в замыкании
	counter := 0      // Данные захватываются в замыкании

	// Возвращаем функцию, которая имеет доступ к mu и counter
	return func() int {
		mu.Lock() // БЛОКИРУЕМ: перед доступом к counter
		defer mu.Unlock()
		counter++ // Критическая секция
		return counter
	}
}

func DemoClosure() {
	increment := CreateProtectedCounter()
	// Снаружи НЕТ доступа к mu и counter - только через функцию!
	// Невозможно забыть сделать Lock - безопасность гарантирована

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			increment() // Внутри автоматически делается Lock/Unlock
		}()
	}
	wg.Wait()
	fmt.Printf("Closure counter: %d\n", increment())
}

// ============================================
// 7. sync.Once - специальный случай
// ============================================
// ЧТО ЗАЩИЩАЕТ: Гарантирует однократную инициализацию instance
// КОГДА ИСПОЛЬЗОВАТЬ: Singleton pattern, ленивая инициализация
// КАК РАБОТАЕТ: Внутри использует mutex + atomic флаг

var (
	instance *Database // Защищается sync.Once
	once     sync.Once // НЕ mutex, но тоже механизм синхронизации
)

type Database struct {
	connection string
}

func GetDatabaseInstance() *Database {
	once.Do(func() {
		// ГАРАНТИЯ: Это выполнится СТРОГО ОДИН раз
		// Даже если 1000 горутин вызовут одновременно
		// sync.Once внутри блокирует остальные горутины
		fmt.Println("Инициализация БД...")
		instance = &Database{connection: "localhost:5432"}
	})
	return instance // Безопасно: instance уже инициализирована
}

func DemoSyncOnce() {
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			db := GetDatabaseInstance() // Все получат ОДИН экземпляр
			fmt.Printf("Горутина %d получила: %v\n", n, db)
		}(i)
	}
	wg.Wait()
}

// ============================================
// 8. Mutex для среза (slice)
// ============================================
// ЧТО ЗАЩИЩАЕТ: Структуру слайса {ptr, len, cap} и базовый массив
// ПОЧЕМУ НУЖЕН: append может изменить ptr и len, чтение может дать некорректные данные
// ВАЖНО: Lock нужен для ЛЮБОЙ операции со слайсом (append, read, len)

type SafeSlice struct {
	mu    sync.Mutex // Защищает items
	items []int      // Слайс НЕ thread-safe!
}

func (s *SafeSlice) Append(item int) {
	s.mu.Lock() // БЛОКИРУЕМ: перед append
	defer s.mu.Unlock()
	s.items = append(s.items, item) // Критическая секция: меняет len/cap/ptr
}

func (s *SafeSlice) Get(index int) (int, bool) {
	s.mu.Lock() // БЛОКИРУЕМ: даже для чтения!
	defer s.mu.Unlock()
	// Критическая секция: читаем len и элемент
	if index < 0 || index >= len(s.items) {
		return 0, false
	}
	return s.items[index], true
}

func (s *SafeSlice) Len() int {
	s.mu.Lock() // БЛОКИРУЕМ: даже для len()!
	defer s.mu.Unlock()
	return len(s.items) // Критическая секция: читаем len из структуры слайса
}

// ============================================
// 9. Когда mutex НЕ нужен - каналы!
// ============================================
// ЧТО ЗАЩИЩАЕТ: Переменную counter через "ownership" (владение)
// ПРИНЦИП: "Don't communicate by sharing memory; share memory by communicating"
// ПРЕИМУЩЕСТВО: counter принадлежит ОДНОЙ горутине, race condition невозможна

func DemoChannelsInsteadOfMutex() {
	// Каналы - это альтернатива mutex для синхронизации
	counterChan := make(chan int)
	resultChan := make(chan int)

	// "Владелец" данных - ТОЛЬКО эта горутина работает с counter
	go func() {
		counter := 0 // НЕ нужен mutex! Никто кроме этой горутины не трогает counter
		for {
			select {
			case counterChan <- counter:
				// Отправляем текущее значение
			case inc := <-counterChan:
				// Получаем запрос на инкремент
				counter += inc // Безопасно: только эта горутина меняет counter
			case resultChan <- counter:
				// Запрос финального значения
				return
			}
		}
	}()

	// Другие горутины НЕ имеют прямого доступа к counter
	// Они общаются через канал - это и есть синхронизация!
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			counterChan <- 1 // Отправляем инкремент через канал
		}()
	}
	wg.Wait()

	result := <-resultChan
	fmt.Printf("Channel counter: %d\n", result)
}

// ============================================
// Запуск всех примеров
// ============================================

func RunMutexPatterns() {
	fmt.Println("=== 1. Mutex in struct ===")
	c := &Counter{}
	c.Inc()
	fmt.Printf("Counter: %d\n", c.value)

	fmt.Println("\n=== 2. Separate mutex ===")
	DemoSeparateMutex()

	fmt.Println("\n=== 3. Package-level mutex ===")
	SetGlobal("key", 42)
	fmt.Printf("Global data: %d\n", GetGlobal("key"))

	fmt.Println("\n=== 4. Multiple variables ===")
	DemoMultipleVariables()

	fmt.Println("\n=== 5. Multiple mutexes ===")
	cd := &ComplexData{}
	cd.IncrementReads()
	cd.IncrementWrites()
	fmt.Printf("Reads: %d, Writes: %d\n", cd.reads, cd.writes)

	fmt.Println("\n=== 6. Closure with mutex ===")
	DemoClosure()

	fmt.Println("\n=== 7. sync.Once ===")
	DemoSyncOnce()

	fmt.Println("\n=== 8. Safe slice ===")
	ss := &SafeSlice{}
	ss.Append(10)
	ss.Append(20)
	fmt.Printf("Slice length: %d\n", ss.Len())

	fmt.Println("\n=== 9. Channels instead of mutex ===")
	DemoChannelsInsteadOfMutex()
}
