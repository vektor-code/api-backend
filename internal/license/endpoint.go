package license

import (
	"os"
	"strings"
)

const defaultActivationEndpoint = "https://activation.cloudraft.net"

func Endpoint() string {
	if ep := strings.TrimRight(strings.TrimSpace(os.Getenv("ACTIVATION_ENDPOINT")), "/"); ep != "" {
		return ep
	}
	return defaultActivationEndpoint
}

func Token() string {
	return strings.TrimSpace(os.Getenv("ACTIVATION_HEARTBEAT_TOKEN"))
}
