package outboxer

import (
	"testing"
)

func TestOutboxMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		scenario string
		function func(*testing.T)
	}{
		{
			"check if NullTime Scan works for message",
			testNullTimeScan,
		},
		{
			"check if NullTime Value works for message",
			testNullTimeValue,
		},
		{
			"check if DynamicValues Scan works for message",
			testDynamicValuesScan,
		},
		{
			"check if DynamicValues Value works for message",
			testDynamicValuesValue,
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			test.function(t)
		})
	}
}

func testNullTimeScan(t *testing.T) {
	nt := NullTime{}
	if err := nt.Scan("03/05/2019"); err != nil {
		t.Fatalf("failed to scan NullTime value: %s", err)
	}

	if err := nt.Scan(nil); err != nil {
		t.Fatalf("failed to scan NullTime with nil value: %s", err)
	}

	if err := nt.Scan("wrongValue"); err != nil {
		t.Fatalf("an error was not expected when scanning a NullTime value: %s", err)
	}
}

func testNullTimeValue(t *testing.T) {
	nt := NullTime{Valid: true}

	v, err := nt.Value()
	if err != nil {
		t.Fatalf("failed to get driver.Value from NullTime: %s", err)
	}

	if v == nil {
		t.Fatalf("driver.Value was expected from NullTime: %s", err)
	}

	nt.Valid = false
	v, _ = nt.Value()
	if v != nil {
		t.Fatalf("driver.Value is not supposed to be nil: %s", err)
	}
}

func testDynamicValuesScan(t *testing.T) {
	dv := DynamicValues{}
	if err := dv.Scan([]byte(`{"key": "value"}`)); err != nil {
		t.Fatalf("failed to scan NullTime value: %s", err)
	}

	if _, ok := dv["key"]; !ok {
		t.Fatal("failed to scan DynamicValue json")
	}

	if dv["key"] != "value" {
		t.Fatal("a string `value` was expected")
	}

	if err := dv.Scan([]byte(``)); err == nil {
		t.Fatal("an error was expected when parsing an empty slice of bytes")
	}

	if err := dv.Scan(nil); err != nil {
		t.Fatal("no error was expected when scanning a nil value")
	}
}

func testDynamicValuesValue(t *testing.T) {
	dv := DynamicValues{}
	dv["key"] = "value"

	v, err := dv.Value()
	if err != nil {
		t.Fatalf("failed to get driver.Value from DynamicValues: %s", err)
	}

	if v == nil {
		t.Fatalf("driver.Value is not supposed to be nil: %s", err)
	}

	dv = DynamicValues{}
	v, err = dv.Value()
	if err != nil {
		t.Fatalf("failed to get driver.Value from DynamicValues: %s", err)
	}

	if v != nil {
		t.Fatalf("driver.Value is supposed to be nil: %s", err)
	}

}
