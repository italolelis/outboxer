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
		t.Fatalf("an error was not expected when scannig a NullTime value: %s", err)
	}
}

func testNullTimeValue(t *testing.T) {
	nt := NullTime{Valid: true}
	if err := nt.Scan("2019-02-01 16:42:35.571831"); err != nil {
		t.Fatalf("failed to scan NullTime value: %s", err)
	}

	v, err := nt.Value()
	if err != nil {
		t.Fatalf("failed to get driver.Value from NullTime: %s", err)
	}

	if v != nil {
		t.Fatalf("driver.Value was expected from NullTime: %s", err)
	}

	nt.Valid = false
	v, _ = nt.Value()
	if v != nil {
		t.Fatalf("driver.Value is not supposed to be nil: %s", err)
	}
}
