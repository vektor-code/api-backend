package license

import (
	"os"
	"strings"
)

const defaultActivationEndpoint = "https://activation.cloudraft.net"

// Same default Activation prod uses when ACTIVATION_HEARTBEAT_TOKEN is unset
// (see crnet-activation platform/deploy/vault/apply-prod.py).
const defaultHeartbeatToken = "crnet-activation-heartbeat-v1"

func Endpoint() string {
	if ep := strings.TrimRight(strings.TrimSpace(os.Getenv("ACTIVATION_ENDPOINT")), "/"); ep != "" {
		return ep
	}
	return defaultActivationEndpoint
}

func Token() string {
	if tok := strings.TrimSpace(os.Getenv("ACTIVATION_HEARTBEAT_TOKEN")); tok != "" {
		return tok
	}
	return defaultHeartbeatToken
}
