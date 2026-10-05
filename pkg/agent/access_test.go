package agent

import (
	"strings"
	"testing"

	"github.com/transactrx/nats-agent/pkg/wire"
)

func TestAccessFromEnvRequiresBoth(t *testing.T) {
	t.Setenv("APP_ID", "")
	t.Setenv("APP_FUNCTION_ID", "")
	if a := accessFromEnv(); a != nil {
		t.Fatalf("want nil when neither APP_ID nor APP_FUNCTION_ID is set, got %+v", a)
	}

	t.Setenv("APP_ID", "x")
	t.Setenv("APP_FUNCTION_ID", "")
	if a := accessFromEnv(); a != nil {
		t.Fatalf("want nil when only APP_ID is set, got %+v", a)
	}

	t.Setenv("APP_ID", "")
	t.Setenv("APP_FUNCTION_ID", "y")
	if a := accessFromEnv(); a != nil {
		t.Fatalf("want nil when only APP_FUNCTION_ID is set, got %+v", a)
	}

	t.Setenv("APP_ID", "x")
	t.Setenv("APP_FUNCTION_ID", "y")
	a := accessFromEnv()
	if a == nil || a.AppID != "x" || a.FunctionID != "y" {
		t.Fatalf("want populated access when both are set, got %+v", a)
	}
}

func TestNewRejectsPartialAccess(t *testing.T) {
	t.Setenv("NATS_URL", "")
	t.Setenv("APP_ID", "")
	t.Setenv("APP_FUNCTION_ID", "")
	_, err := New(Config{Name: "x", Description: "d", Access: &wire.AgentAccess{AppID: "a"}})
	if err == nil {
		t.Fatal("want error for a partial Access (AppID set, FunctionID blank)")
	}
	if !strings.Contains(err.Error(), "FunctionID") {
		t.Fatalf("error should mention FunctionID, got: %v", err)
	}
}

func TestNewRejectsIDTValidationWithoutAccessBeforeNATSConnect(t *testing.T) {
	// No NATS_URL is set (or reachable) in this test process; if the
	// misconfig check ran after the connect attempt this would instead fail
	// with a connection error, not the intended message.
	t.Setenv("NATS_URL", "")
	t.Setenv("APP_ID", "")
	t.Setenv("APP_FUNCTION_ID", "")
	_, err := New(Config{
		Name:          "x",
		Description:   "d",
		IDTValidation: &IDTValidation{Enabled: true},
	})
	if err == nil {
		t.Fatalf("want error for IDT_VALIDATION=true with no Access")
	}
	if !strings.Contains(err.Error(), "APP_ID") {
		t.Fatalf("error should mention APP_ID, got: %v", err)
	}
}
