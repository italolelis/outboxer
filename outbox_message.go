package outboxer

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
	"time"
)

// OutboxMessage represents a message that will be sent
type OutboxMessage struct {
	ID           int64
	Dispatched   bool
	DispatchedAt time.Time
	Payload      []byte
	Options      DynamicValues
	Headers      DynamicValues
}

// DynamicValues is a map that can be serialized
type DynamicValues map[string]interface{}

// Value return a driver.Value representation of the order items
func (p DynamicValues) Value() (driver.Value, error) {
	if len(p) == 0 {
		return nil, nil
	}
	return json.Marshal(p)
}

// Scan scans a database json representation into a []Item
func (p *DynamicValues) Scan(src interface{}) error {
	v := reflect.ValueOf(src)
	if !v.IsValid() || v.IsNil() {
		return nil
	}
	if data, ok := src.([]byte); ok {
		return json.Unmarshal(data, &p)
	}
	return fmt.Errorf("could not not decode type %T -> %T", src, p)
}
