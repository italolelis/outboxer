package outboxer

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"
)

// OutboxMessage represents a message that will be sent
type OutboxMessage struct {
	ID           int64
	Dispatched   bool
	DispatchedAt NullTime
	Payload      []byte
	Options      DynamicValues
	Headers      DynamicValues
}

// NullTime represents a nullable time.Time
type NullTime struct {
	Time  time.Time
	Valid bool // Valid is true if Time is not NULL
}

// Scan implements the Scanner interface.
func (nt *NullTime) Scan(value interface{}) error {
	nt.Time, nt.Valid = value.(time.Time)
	return nil
}

// Value implements the driver Valuer interface.
func (nt NullTime) Value() (driver.Value, error) {
	if !nt.Valid {
		return nil, nil
	}
	return nt.Time, nil
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
	if data, ok := src.([]byte); ok {
		return json.Unmarshal(data, &p)
	}
	return fmt.Errorf("could not not decode type %T -> %T", src, p)
}
