package natsagent_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/transactrx/nats-agent/pkg/tool"
	"github.com/transactrx/nats-agent/pkg/wire"
)

// TestCLI builds cmd/nats-agents and drives it against the embedded server
// with a live agent and tool host — the CLI is exercised exactly as an
// operator would use it.
func TestCLI(t *testing.T) {
	startTestAgent(t)
	h := tool.NewHostWithNATS(testURL, "", "")
	if err := h.Register(echoTool{}, nil); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := h.Start(); err != nil {
		t.Fatalf("host.Start: %v", err)
	}
	t.Cleanup(h.Shutdown)

	bin := filepath.Join(t.TempDir(), "nats-agents")
	if out, err := exec.Command("go", "build", "-o", bin, "./cmd/nats-agents").CombinedOutput(); err != nil {
		t.Fatalf("building CLI: %v\n%s", err, out)
	}

	cli := func(args ...string) (string, string, error) {
		cmd := exec.Command(bin, append([]string{"-s", testURL, "--timeout", "700ms"}, args...)...)
		var stdout, stderr strings.Builder
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		err := cmd.Run()
		return stdout.String(), stderr.String(), err
	}

	t.Run("list", func(t *testing.T) {
		out, _, err := cli("list")
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		if !strings.Contains(out, "testAgent") || !strings.Contains(out, "sessions") {
			t.Errorf("list output missing agent info:\n%s", out)
		}
	})

	t.Run("tools", func(t *testing.T) {
		out, _, err := cli("tools", "--json")
		if err != nil {
			t.Fatalf("tools: %v", err)
		}
		var cards []wire.ToolCard
		if err := json.Unmarshal([]byte(out), &cards); err != nil {
			t.Fatalf("tools --json not parseable: %v\n%s", err, out)
		}
		if len(cards) == 0 || cards[0].Name != "echo" {
			t.Errorf("echo tool not discovered: %s", out)
		}
	})

	t.Run("card and ping", func(t *testing.T) {
		out, _, err := cli("card", "testAgent")
		if err != nil || !strings.Contains(out, `"protocolVersion"`) {
			t.Errorf("card failed: %v\n%s", err, out)
		}
		out, _, err = cli("ping", "testAgent")
		if err != nil || !strings.Contains(out, `"ok"`) {
			t.Errorf("ping failed: %v\n%s", err, out)
		}
	})

	t.Run("chat", func(t *testing.T) {
		out, stderr, err := cli("chat", "testAgent", "hello cli")
		if err != nil {
			t.Fatalf("chat: %v\nstderr: %s", err, stderr)
		}
		if !strings.Contains(out, "you said: hello cli") {
			t.Errorf("chat reply missing:\nstdout: %s", out)
		}
		if !strings.Contains(stderr, "[tool] echo") || !strings.Contains(stderr, "[done] endTurn") {
			t.Errorf("chat progress events missing:\nstderr: %s", stderr)
		}
	})

	t.Run("nats context resolution", func(t *testing.T) {
		// A fake HOME with a nats CLI context pointing at the embedded
		// server; no -s flag and no NATS_URL in the environment.
		home := t.TempDir()
		ctxDir := filepath.Join(home, ".config", "nats", "context")
		if err := os.MkdirAll(ctxDir, 0o755); err != nil {
			t.Fatal(err)
		}
		ctxJSON := []byte(`{"url":"` + testURL + `","description":"embedded test server"}`)
		if err := os.WriteFile(filepath.Join(ctxDir, "embedded.json"), ctxJSON, 0o644); err != nil {
			t.Fatal(err)
		}
		env := []string{"HOME=" + home, "PATH=" + os.Getenv("PATH")}

		// Explicit --context
		cmd := exec.Command(bin, "--context", "embedded", "--timeout", "700ms", "list")
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		if err != nil || !strings.Contains(string(out), "testAgent") {
			t.Errorf("--context list failed: %v\n%s", err, out)
		}

		// The context selected via `nats context select` (context.txt) is
		// used with no options at all — and wins over $NATS_URL.
		if err := os.WriteFile(filepath.Join(home, ".config", "nats", "context.txt"), []byte("embedded\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		cmd = exec.Command(bin, "--timeout", "700ms", "tools")
		cmd.Env = append(env, "NATS_URL=nats://selected-context-should-win.invalid:4222")
		out, err = cmd.CombinedOutput()
		if err != nil || !strings.Contains(string(out), "echo") {
			t.Errorf("selected-context tools failed (context must beat $NATS_URL): %v\n%s", err, out)
		}
	})

	t.Run("run tool", func(t *testing.T) {
		out, _, err := cli("run", "echo", `{"say":"cli"}`)
		if err != nil {
			t.Fatalf("run: %v\n%s", err, out)
		}
		var resp wire.ToolRunResponse
		if err := json.Unmarshal([]byte(out), &resp); err != nil {
			t.Fatalf("run output not JSON: %v\n%s", err, out)
		}
		if resp.Status != wire.ToolStatusSuccess || !strings.Contains(resp.Content[0].Text, "cli") {
			t.Errorf("bad run response: %s", out)
		}
	})
}
