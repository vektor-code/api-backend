package license

import (
	"os"
	"strings"
)

func Endpoint() string {
	return strings.TrimRight(strings.TrimSpace(os.Getenv("ACTIVATION_ENDPOINT")), "/")
}

func Token() string {
	return strings.TrimSpace(os.Getenv("ACTIVATION_HEARTBEAT_TOKEN"))
}
