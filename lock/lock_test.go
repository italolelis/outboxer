package lock

import "testing"

func TestLock_Generate(t *testing.T) {
	generatedLock := "233322130"

	aid, err := Generate("test", "extraKey")
	if err != nil {
		t.Fatalf("failed to generate an advisory lock: %s", err)
	}

	if aid == "" {
		t.Fatal("the advisory lock cannot be empty")
	}

	if aid != generatedLock {
		t.Fatalf("the expected lock was %s but got %s", generatedLock, aid)
	}
}
