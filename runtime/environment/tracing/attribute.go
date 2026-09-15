package tracing

import "math"

// AttributeType identifies the scalar stored in an AttributeValue.
type AttributeType uint8

const (
	InvalidAttribute AttributeType = iota
	StringAttribute
	Int64Attribute
	Float64Attribute
	BoolAttribute
)

// AttributeValue stores a scalar without interface boxing or heap-owned wrappers.
// Values are constructed by StringAttr, Int64Attr, Float64Attr and BoolAttr.
// The zero value is invalid. Accessors return zero for a mismatched type.
type AttributeValue struct {
	text string
	bits uint64
	kind AttributeType
}

func (v AttributeValue) Type() AttributeType { return v.kind }

func (v AttributeValue) AsString() string {
	if v.kind == StringAttribute {
		return v.text
	}
	return ""
}

func (v AttributeValue) AsInt64() int64 {
	if v.kind == Int64Attribute {
		return int64(v.bits)
	}
	return 0
}

func (v AttributeValue) AsFloat64() float64 {
	if v.kind == Float64Attribute {
		return math.Float64frombits(v.bits)
	}
	return 0
}

func (v AttributeValue) AsBool() bool {
	return v.kind == BoolAttribute && v.bits != 0
}

// Attribute is a homogeneous key/value pair for span and event attributes.
// Its payload is typed: a mixed []Attribute needs no interface conversions.
type Attribute struct {
	Key   string
	Value AttributeValue
}

func StringAttr(key, value string) Attribute {
	return Attribute{Key: key, Value: AttributeValue{kind: StringAttribute, text: value}}
}

func Int64Attr(key string, value int64) Attribute {
	return Attribute{Key: key, Value: AttributeValue{kind: Int64Attribute, bits: uint64(value)}}
}

func Float64Attr(key string, value float64) Attribute {
	return Attribute{Key: key, Value: AttributeValue{kind: Float64Attribute, bits: math.Float64bits(value)}}
}

func BoolAttr(key string, value bool) Attribute {
	var bits uint64
	if value {
		bits = 1
	}
	return Attribute{Key: key, Value: AttributeValue{kind: BoolAttribute, bits: bits}}
}
