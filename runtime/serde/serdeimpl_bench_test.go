package serde

import (
	"testing"
)

func BenchmarkStringSerde(b *testing.B) {
	s := &StringSerde{}
	v := "hello, streaming world"
	buf := make([]byte, 0, 64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = buf[:0]
		var err error
		buf, err = s.Serialize(v, buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInt64Serde(b *testing.B) {
	s := &Int64Serde{}
	buf := make([]byte, 0, 16)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = buf[:0]
		var err error
		buf, err = s.Serialize(int64(i), buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFloat64Serde(b *testing.B) {
	s := &Float64Serde{}
	buf := make([]byte, 0, 16)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = buf[:0]
		var err error
		buf, err = s.Serialize(float64(i)*1.1, buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIntArraySerde(b *testing.B) {
	s := &IntArraySerde{}
	v := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	buf := make([]byte, 0, 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = buf[:0]
		var err error
		buf, err = s.Serialize(v, buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStringSerdeNoReuse(b *testing.B) {
	s := &StringSerde{}
	v := "hello, streaming world"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := s.Serialize(v, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInt64SerdeNoReuse(b *testing.B) {
	s := &Int64Serde{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := s.Serialize(int64(i), nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}
