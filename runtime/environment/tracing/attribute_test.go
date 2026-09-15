package tracing

import (
	"math"
	"strconv"
	"testing"
)

func TestAttributeScalarFidelity(t *testing.T) {
	for _, value := range []string{"", "booking", "\x00UTF-8:\xc3\xa9"} {
		attr := StringAttr("key", value)
		if attr.Key != "key" || attr.Value.Type() != StringAttribute || attr.Value.AsString() != value {
			t.Fatalf("string round-trip: %+v", attr)
		}
	}
	for _, value := range []int64{0, -1, 256, math.MinInt64, math.MaxInt64} {
		attr := Int64Attr("key", value)
		if attr.Value.Type() != Int64Attribute || attr.Value.AsInt64() != value {
			t.Fatalf("int64 round-trip: %+v", attr)
		}
	}
	for _, bits := range []uint64{0, 1 << 63, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000123, 1, math.Float64bits(-12.5)} {
		attr := Float64Attr("key", math.Float64frombits(bits))
		if attr.Value.Type() != Float64Attribute || math.Float64bits(attr.Value.AsFloat64()) != bits {
			t.Fatalf("float64 bits not preserved: %x", bits)
		}
	}
	for _, value := range []bool{false, true} {
		attr := BoolAttr("key", value)
		if attr.Value.Type() != BoolAttribute || attr.Value.AsBool() != value {
			t.Fatalf("bool round-trip: %+v", attr)
		}
	}
	if (Attribute{}).Value.Type() != InvalidAttribute {
		t.Fatal("zero attribute must be invalid")
	}
	if Int64Attr("", 42).Value.AsString() != "" || StringAttr("", "42").Value.AsInt64() != 0 ||
		Int64Attr("", 42).Value.AsFloat64() != 0 || Int64Attr("", 1).Value.AsBool() {
		t.Fatal("mismatched accessor must return zero")
	}
}

var storedAttributes [4]Attribute

// Global storage forces values to survive the call, unlike a stack-only example
// in which escape analysis may eliminate even the old interface boxing.
func TestAttributeConstructionDoesNotAllocate(t *testing.T) {
	value := strconv.Itoa(1234567)
	allocations := testing.AllocsPerRun(1000, func() {
		storedAttributes = [4]Attribute{StringAttr("s", value), Int64Attr("i", 1234567), Float64Attr("f", 12.5), BoolAttr("b", true)}
	})
	if allocations != 0 {
		t.Fatalf("attribute payloads allocated: %g", allocations)
	}
}

type boxedAttribute struct {
	Key   string
	Value interface{}
}

var storedBoxedAttributes [4]boxedAttribute

func BenchmarkAttributePayloads(b *testing.B) {
	value := strconv.Itoa(1234567)
	b.Run("boxed", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			storedBoxedAttributes = [4]boxedAttribute{{"s", value}, {"i", int64(i + 1024)}, {"f", float64(i) + 0.5}, {"b", i%2 == 0}}
		}
	})
	b.Run("typed", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			storedAttributes = [4]Attribute{StringAttr("s", value), Int64Attr("i", int64(i+1024)), Float64Attr("f", float64(i)+0.5), BoolAttr("b", i%2 == 0)}
		}
	})
}
