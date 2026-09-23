package errors // Создание своей ошибки

import (
	"errors"
	"fmt"
)

var SomeError = errors.New("some error")

func foo() error {
	var result error

	result = SomeError

	return result
}

// RunErrorExamples - демонстрационная функция для вызова из main
func RunErrorExamples() {
	fmt.Println("\n=== Пример: Создание своей ошибки ===")
	result := foo()

	if result != nil {
		fmt.Println("Error occured!!!", result)
		return
	}
	fmt.Println("No error")
}
