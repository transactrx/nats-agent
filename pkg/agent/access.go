package agent

import (
	"log"
	"os"
	"strings"

	"github.com/transactrx/nats-agent/pkg/idt"
	"github.com/transactrx/nats-agent/pkg/wire"
)

// IDTValidation configures inbound Internal Delegation Token checks. It is
// the shared idt.Validation, kept under its original name for agent code.
type IDTValidation = idt.Validation

// Identity is the runtime's verdict on the caller of a request.
type Identity = idt.Identity

// IDTValidationFromEnv reads the org env contract (see idt.ValidationFromEnv).
func IDTValidationFromEnv() IDTValidation { return idt.ValidationFromEnv() }

// accessFromEnv is the Config.Access fallback (APP_ID / APP_FUNCTION_ID).
// Returns nil unless BOTH are set: a partial declaration (e.g. APP_ID with no
// APP_FUNCTION_ID) would otherwise advertise a card with an empty
// functionId, which downstream identity checks treat as BAD_REQUEST for
// every caller.
func accessFromEnv() *wire.AgentAccess {
	appID := strings.TrimSpace(os.Getenv("APP_ID"))
	fnID := strings.TrimSpace(os.Getenv("APP_FUNCTION_ID"))
	if appID == "" && fnID == "" {
		return nil
	}
	if appID == "" || fnID == "" {
		log.Printf("IDT_METRIC event=access.misconfig reason=partial_env")
		return nil
	}
	return &wire.AgentAccess{AppID: appID, FunctionID: fnID}
}
