package outboxer_test

import (
	"strings"
	"testing"

	"github.com/italolelis/outboxer"
)

func TestOutboxMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		function func(*testing.T)
		scenario string
	}{
		{
			testDynamicValuesScan,
			"check if DynamicValues Scan works for message",
		},
		{
			testDynamicValuesValue,
			"check if DynamicValues Value works for message",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.scenario, func(t *testing.T) {
			test.function(t)
		})
	}
}

func testDynamicValuesScan(t *testing.T) {
	dv := outboxer.DynamicValues{}
	if err := dv.Scan([]byte(`{"key": "value"}`)); err != nil {
		t.Fatalf("failed to scan DynamicValues value: %s", err)
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
	dv := outboxer.DynamicValues{}
	dv["key"] = "value"

	v, err := dv.Value()
	if err != nil {
		t.Fatalf("failed to get driver.Value from DynamicValues: %s", err)
	}

	if v == nil {
		t.Fatalf("driver.Value is not supposed to be nil: %s", err)
	}

	dv = outboxer.DynamicValues{}

	v, err = dv.Value()
	if err != nil {
		t.Fatalf("failed to get driver.Value from DynamicValues: %s", err)
	}

	if v != nil {
		t.Fatalf("driver.Value is supposed to be nil: %s", err)
	}
}

func TestDynamicValues_Scan_InvalidType(t *testing.T) {
	p := &outboxer.DynamicValues{}

	// call Scan with an invalid type
	err := p.Scan(123)

	// assert that the error is not nil
	if err == nil {
		t.Error("expected an error, but got nil")
	}

	// assert that the error message contains the expected text
	expectedErrMsg := "could not not decode type int -> *outboxer.DynamicValues: failed to decode type"
	if !strings.Contains(err.Error(), expectedErrMsg) {
		t.Errorf("expected error message to contain %q, but got %q", expectedErrMsg, err.Error())
	}
}

func TestDynamicValues_Scan_InvalidSrc(t *testing.T) {
	p := &outboxer.DynamicValues{}

	// call Scan with an invalid src
	err := p.Scan(nil)

	// assert that the error is nil
	if err != nil {
		t.Errorf("expected nil error, but got %s", err)
	}

	// call Scan with an invalid src
	err = p.Scan((*int)(nil))

	// assert that the error is not nil
	if err == nil {
		t.Error("expected an error, but got nil")
	}

	// assert that the error message contains the expected text
	expectedErrMsg := "could not not decode type *int -> *outboxer.DynamicValues: failed to decode type"
	if !strings.Contains(err.Error(), expectedErrMsg) {
		t.Errorf("expected error message to contain %q, but got %q", expectedErrMsg, err.Error())
	}
}
