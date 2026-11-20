package agent

import "testing"

func TestConfigDefaults(t *testing.T) {
	cases := []struct {
		name    string
		env     string
		set     bool
		in      Config
		wantMax int
		wantOrc bool
	}{
		{"zero max defaults to 1", "", false, Config{}, 1, false},
		{"negative max defaults to 1", "", false, Config{MaxDelegationDepth: -2}, 1, false},
		{"explicit max kept", "", false, Config{MaxDelegationDepth: 3}, 3, false},
		{"env true", "true", true, Config{}, 1, true},
		{"env TRUE with spaces", " TRUE ", true, Config{}, 1, true},
		{"env false", "false", true, Config{}, 1, false},
		{"field true wins over env false", "false", true, Config{Orchestrator: true}, 1, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.set {
				t.Setenv("AGENT_ORCHESTRATOR", c.env)
			} else {
				t.Setenv("AGENT_ORCHESTRATOR", "")
			}
			cfg := c.in
			applyDelegationDefaults(&cfg)
			if cfg.MaxDelegationDepth != c.wantMax || cfg.Orchestrator != c.wantOrc {
				t.Fatalf("got max=%d orch=%v, want max=%d orch=%v", cfg.MaxDelegationDepth, cfg.Orchestrator, c.wantMax, c.wantOrc)
			}
		})
	}
}
