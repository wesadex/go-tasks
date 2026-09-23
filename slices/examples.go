package slices

import "fmt"

// ═══════════════════════════════════════════════════════════════════
// Пример 1: Modify slice (из main.go5)
// ═══════════════════════════════════════════════════════════════════

func modify(s []int, n int) {
	s = append(s, n)
	s[0] = 999
}

func DemoModifySlice() {
	fmt.Println("\n=== Пример 1: Modify Slice ===")
	s1 := make([]int, 3, 5)
	s2 := s1[:2]

	s1[0] = 1
	s2[1] = 2

	modify(s1, 55)
	modify(s2, 66)

	fmt.Println("s1:", s1)
	fmt.Println("s2:", s2)
	fmt.Println("s1 cap:", cap(s1), "s2 cap:", cap(s2))

	s3 := s2[:5]
	fmt.Println("s3:", s3)
}

// ═══════════════════════════════════════════════════════════════════
// Пример 2-4: Другие примеры слайсов (нужно добавить из других main.go)
// ═══════════════════════════════════════════════════════════════════

// RunAllExamples - запускает все примеры слайсов
func RunAllExamples() {
	fmt.Println("\n╔═══════════════════════════════════════════╗")
	fmt.Println("║        ПРИМЕРЫ СО СЛАЙСАМИ                ║")
	fmt.Println("╚═══════════════════════════════════════════╝")
	
	DemoModifySlice()
}

