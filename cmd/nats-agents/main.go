// nats-agents is the operator CLI for the NATS Agent Protocol: discover the
// agents and network tools on the mesh, inspect their cards, check liveness,
// run a streaming chat turn against an agent, or execute a tool — the same
// way another agent would.
//
// Connection follows the org conventions: NATS_URL / NATS_JWT / NATS_KEY from
// the environment, overridable with flags (or a .creds file).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/transactrx/nats-agent/pkg/agentclient"
	"github.com/transactrx/nats-agent/pkg/toolclient"
	"github.com/transactrx/nats-agent/pkg/wire"
)

var version = "dev" // set via ldflags: -X main.version=...

const usageText = `nats-agents — discover and drive NATS Agent Protocol agents and tools

Usage:
  nats-agents [flags] <command> [args]

Commands:
  list                  Discover agents on the mesh (agent cards)
  tools                 Discover network tools on the mesh (tool cards)
  card <agent>          Fetch one agent's full card
  tool <name>           Fetch one tool's full card
  ping <agent>          Agent liveness probe
  chat <agent> <text>   Run one streaming chat turn; prints the reply live
  run <tool> <json>     Execute a network tool with a JSON input object

Flags:
  -s, --server URL      NATS server URL (default: $NATS_URL)
      --creds FILE      NATS .creds file (JWT + seed)
      --jwt STRING      NATS user JWT (default: $NATS_JWT)
      --seed STRING     NATS user NKey seed (default: $NATS_KEY)
      --timeout DUR     Discovery window / request timeout (default 3s)
      --json            Raw JSON output instead of tables
      --version         Print version and exit

Examples:
  nats-agents list
  nats-agents tools --json
  nats-agents chat copayAssistant "How many claims failed yesterday?"
  nats-agents run create_chart '{"chart":{"type":"bar","data":{"labels":["A"],"datasets":[{"data":[1]}]}}}'
`

type config struct {
	server  string
	creds   string
	jwt     string
	seed    string
	timeout time.Duration
	asJSON  bool
}

func main() {
	cfg := config{}
	showVersion := false

	fs := flag.NewFlagSet("nats-agents", flag.ExitOnError)
	fs.Usage = func() { fmt.Fprint(os.Stderr, usageText) }
	fs.StringVar(&cfg.server, "s", "", "")
	fs.StringVar(&cfg.server, "server", "", "")
	fs.StringVar(&cfg.creds, "creds", "", "")
	fs.StringVar(&cfg.jwt, "jwt", "", "")
	fs.StringVar(&cfg.seed, "seed", "", "")
	fs.DurationVar(&cfg.timeout, "timeout", 3*time.Second, "")
	fs.BoolVar(&cfg.asJSON, "json", false, "")
	fs.BoolVar(&showVersion, "version", false, "")
	_ = fs.Parse(os.Args[1:])

	if showVersion {
		fmt.Printf("nats-agents version %s\n", version)
		return
	}
	args := fs.Args()
	if len(args) == 0 {
		fs.Usage()
		os.Exit(2)
	}
	// Flags are accepted before or after the command: re-parse the remainder
	// so `nats-agents tools --json` works like `nats-agents --json tools`.
	cmd := args[0]
	_ = fs.Parse(args[1:])
	rest := fs.Args()

	nc, err := connect(cfg)
	if err != nil {
		fatal("connecting to NATS: %v", err)
	}
	defer nc.Close()

	ac := agentclient.NewFromConn(nc)
	tc := toolclient.NewFromConn(nc)
	ctx := context.Background()

	switch cmd {
	case "list":
		cards, err := ac.Discover(ctx, wire.DiscoverFilter{}, cfg.timeout)
		exitIf(err)
		printAgentCards(cards, cfg.asJSON)
	case "tools":
		cards, err := tc.Discover(ctx, wire.DiscoverFilter{}, cfg.timeout)
		exitIf(err)
		printToolCards(cards, cfg.asJSON)
	case "card":
		requireArg(rest, 1, "card <agent>")
		card, err := ac.Card(ctx, rest[0])
		exitIf(err)
		printJSON(card)
	case "tool":
		requireArg(rest, 1, "tool <name>")
		card, err := tc.Card(ctx, rest[0])
		exitIf(err)
		printJSON(card)
	case "ping":
		requireArg(rest, 1, "ping <agent>")
		resp, err := ac.Ping(ctx, rest[0])
		exitIf(err)
		printJSON(resp)
	case "chat":
		requireArg(rest, 2, "chat <agent> <text>")
		chat(ctx, ac, rest[0], strings.Join(rest[1:], " "), cfg)
	case "run":
		requireArg(rest, 2, "run <tool> <json-input>")
		runTool(ctx, tc, rest[0], rest[1], cfg)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", cmd)
		fs.Usage()
		os.Exit(2)
	}
}

func connect(cfg config) (*nats.Conn, error) {
	url := cfg.server
	if url == "" {
		url = os.Getenv("NATS_URL")
	}
	if url == "" {
		return nil, fmt.Errorf("no NATS server: set -s/--server or $NATS_URL")
	}
	var opts []nats.Option
	switch {
	case cfg.creds != "":
		opts = append(opts, nats.UserCredentials(cfg.creds))
	default:
		jwt := cfg.jwt
		if jwt == "" {
			jwt = os.Getenv("NATS_JWT")
		}
		seed := cfg.seed
		if seed == "" {
			seed = os.Getenv("NATS_KEY")
		}
		if jwt != "" && seed != "" {
			opts = append(opts, nats.UserJWTAndSeed(jwt, seed))
		}
	}
	opts = append(opts, nats.Name("nats-agents-cli"))
	return nats.Connect(url, opts...)
}

func chat(ctx context.Context, ac *agentclient.Client, agent, text string, cfg config) {
	run, err := ac.Chat(ctx, agent, wire.ChatRequest{
		Message: wire.Message{Role: "user", Content: []wire.ContentBlock{{Text: text}}},
	})
	exitIf(err)
	fmt.Fprintf(os.Stderr, "run %s session %s (instance %s)\n", run.Ack.RunID, run.Ack.SessionID, run.Ack.InstanceID)

	for ev := range run.Events {
		if cfg.asJSON {
			printJSON(ev)
			continue
		}
		switch ev.Type {
		case wire.EventText:
			fmt.Print(ev.TextDelta)
		case wire.EventStatus:
			fmt.Fprintf(os.Stderr, "[status] %s\n", ev.StatusText)
		case wire.EventToolUse:
			fmt.Fprintf(os.Stderr, "[tool] %s %s\n", ev.ToolName, compact(ev.ToolInput, 120))
		case wire.EventToolResult:
			outcome := "ok"
			if ev.ToolError != "" {
				outcome = "error: " + ev.ToolError
			}
			fmt.Fprintf(os.Stderr, "[tool] %s → %s\n", ev.ToolName, outcome)
		case wire.EventData:
			fmt.Fprintf(os.Stderr, "[data:%s] %s\n", ev.Kind, compact(ev.Payload, 200))
		case wire.EventDone:
			fmt.Printf("\n")
			fmt.Fprintf(os.Stderr, "[done] %s\n", ev.StopReason)
		case wire.EventError:
			fmt.Printf("\n")
			fatal("[error] %s", ev.Error)
		}
	}
}

func runTool(ctx context.Context, tc *toolclient.Client, name, rawInput string, cfg config) {
	var input map[string]any
	if err := json.Unmarshal([]byte(rawInput), &input); err != nil {
		fatal("input is not a JSON object: %v", err)
	}
	// Size the request timeout from the tool's own advertisement when we can.
	timeout := cfg.timeout
	if card, err := tc.Card(ctx, name); err == nil && card.TimeoutSeconds > 0 {
		timeout = time.Duration(card.TimeoutSeconds)*time.Second + 10*time.Second
	}
	resp, err := tc.Run(ctx, name, wire.ToolRunRequest{Input: input, Agent: "nats-agents-cli"}, timeout)
	exitIf(err)
	printJSON(resp)
	if resp.Status == wire.ToolStatusError {
		os.Exit(1)
	}
}

func printAgentCards(cards []wire.AgentCard, asJSON bool) {
	sort.Slice(cards, func(i, j int) bool { return cards[i].Name < cards[j].Name })
	if asJSON {
		printJSON(cards)
		return
	}
	if len(cards) == 0 {
		fmt.Println("no agents responded")
		return
	}
	w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintln(w, "AGENT\tVERSION\tCAPABILITIES\tSKILLS\tDESCRIPTION")
	for _, c := range cards {
		caps := []string{"chat"}
		if c.Capabilities.Sync {
			caps = append(caps, "sync")
		}
		if c.Capabilities.Sessions {
			caps = append(caps, "sessions")
		}
		skills := make([]string, 0, len(c.Skills))
		for _, s := range c.Skills {
			skills = append(skills, s.Name)
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
			c.Name, c.Version, strings.Join(caps, ","), strings.Join(skills, ","), truncate(c.Description, 60))
	}
	_ = w.Flush()
}

func printToolCards(cards []wire.ToolCard, asJSON bool) {
	sort.Slice(cards, func(i, j int) bool { return cards[i].Name < cards[j].Name })
	if asJSON {
		printJSON(cards)
		return
	}
	if len(cards) == 0 {
		fmt.Println("no tools responded")
		return
	}
	w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintln(w, "TOOL\tVERSION\tTIMEOUT\tTAGS\tDESCRIPTION")
	for _, c := range cards {
		fmt.Fprintf(w, "%s\t%s\t%ds\t%s\t%s\n",
			c.Name, c.Version, c.TimeoutSeconds, strings.Join(c.Tags, ","), truncate(c.Description, 60))
	}
	_ = w.Flush()
}

func printJSON(v any) {
	out, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fatal("marshaling output: %v", err)
	}
	fmt.Println(string(out))
}

func compact(raw json.RawMessage, n int) string {
	return truncate(string(raw), n)
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func requireArg(args []string, n int, usage string) {
	if len(args) < n {
		fatal("usage: nats-agents %s", usage)
	}
}

func exitIf(err error) {
	if err != nil {
		fatal("%v", err)
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
