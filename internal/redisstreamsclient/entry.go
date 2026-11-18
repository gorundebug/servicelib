package redisstreamsclient

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"
)

// Field is a binary-safe Redis field/value pair. Strings may contain arbitrary bytes.
type Field struct{ Name, Value string }

// Entry preserves native order and repeated field names in Fields.
// Values is a convenience last-value map; use Fields for lossless forwarding.
type Entry struct {
	redis.XMessage
	Fields []Field
	epoch  uint64
}

func decodeEntries(raw interface{}) ([]Entry, error) {
	rows, ok := raw.([]interface{})
	if !ok {
		return nil, errors.New("invalid Redis entry list")
	}
	entries := make([]Entry, 0, len(rows))
	for _, row := range rows {
		parts, ok := row.([]interface{})
		if !ok || len(parts) != 2 {
			return nil, errors.New("invalid Redis entry")
		}
		id, ok := parts[0].(string)
		if !ok {
			return nil, errors.New("invalid Redis entry ID")
		}
		values, ok := parts[1].([]interface{})
		if !ok || len(values)%2 != 0 {
			return nil, errors.New("invalid Redis field/value list")
		}
		entry := Entry{XMessage: redis.XMessage{ID: id, Values: make(map[string]interface{})}}
		for i := 0; i < len(values); i += 2 {
			name, nok := values[i].(string)
			value, vok := values[i+1].(string)
			if !nok || !vok {
				return nil, errors.New("invalid Redis field/value bytes")
			}
			entry.Fields = append(entry.Fields, Field{name, value})
			entry.Values[name] = value
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// PublishFields retains native order and duplicate keys, without retrying XADD.
func (s *Session) PublishFields(ctx context.Context, fields []Field) (string, error) {
	if len(fields) == 0 {
		return "", errors.New("Redis publication requires field/value records")
	}
	values := make([]interface{}, 0, len(fields)*2)
	for _, field := range fields {
		values = append(values, field.Name, field.Value)
	}
	op, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.commands.XAdd(op, &redis.XAddArgs{Stream: s.endpoint.StreamKey, NoMkStream: !s.endpoint.CreateStream, ID: "*", Values: values}).Result()
}
