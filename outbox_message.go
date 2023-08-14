package outboxer

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

// ErrFailedToDecodeType is returned when the type of the value is not supported.
var ErrFailedToDecodeType = errors.New("failed to decode type")

// OutboxMessage represents a message that will be sent.
type OutboxMessage struct {
	Options      DynamicValues
	Headers      DynamicValues
	DispatchedAt sql.NullTime
	Payload      []byte
	ID           int64
	Dispatched   bool
}

// DynamicValues is a map that can be serialized.
type DynamicValues map[string]interface{}

// Value return a driver.Value representation of the order items.
func (p DynamicValues) Value() (driver.Value, error) {
	if len(p) == 0 {
		return nil, nil
	}

	return json.Marshal(p)
}

// Scan scans a database json representation into a []Item.
func (p *DynamicValues) Scan(src interface{}) error {
	if src == nil {
		return nil
	}

	v := reflect.ValueOf(src)
	if !v.IsValid() {
		return nil
	}

	if data, ok := src.([]byte); ok {
		return json.Unmarshal(data, &p)
	}

	return fmt.Errorf("could not not decode type %T -> %T: %w", src, p, ErrFailedToDecodeType)
}
