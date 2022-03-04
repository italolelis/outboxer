package lock

import (
	"fmt"
	"hash/crc32"
	"strings"
)

const advisoryLockIDSalt uint = 1486364155

// Generate generates an advisory lock.
func Generate(databaseName string, additionalNames ...string) (string, error) {
	if len(additionalNames) > 0 {
		databaseName = strings.Join(append(additionalNames, databaseName), "\x00")
	}

	sum := crc32.ChecksumIEEE([]byte(databaseName))
	sum *= uint32(advisoryLockIDSalt)

	return fmt.Sprintf("%v", sum), nil
}
