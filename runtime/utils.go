/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"regexp"
	"strings"
)

var reSplit = regexp.MustCompile(`[A-Z][^A-Z\s_-]*|[^A-Z\s_-]+`)

func splitWords(s string) []string {
	return reSplit.FindAllString(s, -1)
}

func ToSnakeCase(text string) string {
	words := splitWords(text)
	for i := range words {
		words[i] = strings.ToLower(words[i])
	}
	return strings.Join(words, "_")
}

type Collection[T any] struct {
	data []T
}

func MakeCollection[T any](data []T) Collection[T] {
	return Collection[T]{data: data}
}

func (s Collection[T]) Len() int {
	return len(s.data)
}

func (s Collection[T]) At(i int) T {
	return s.data[i]
}
