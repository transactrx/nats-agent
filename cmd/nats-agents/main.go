// nats-agents is the operator CLI for the NATS Agent Protocol: discover the
// agents and network tools on the mesh, inspect their cards, check liveness,
// run a streaming chat turn against an agent, or execute a tool — the same
// way another agent would.
//
// Connection follows the org conventions: NATS_URL / NATS_JWT / NATS_KEY from
// the environment, overridable with flags (or a .creds file).
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
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
  chat <agent> [text]   Chat with an agent; prints the reply live. With no
                        text, starts an interactive multi-turn conversation
                        (the agent keeps context between turns)
  run <tool> <json>     Execute a network tool with a JSON input object

Flags:
  -s, --server URL      NATS server URL (default: $NATS_URL)
      --context NAME    NATS CLI context (from ~/.config/nats/context);
                        the selected default context is used when nothing
                        else specifies a server
      --creds FILE      NATS .creds file (JWT + seed)
      --nkey FILE       NATS NKey seed file
      --jwt STRING      NATS user JWT (default: $NATS_JWT)
      --seed STRING     NATS user NKey seed (default: $NATS_KEY)
      --timeout DUR     Discovery window / request timeout (default 3s)
      --json            Raw JSON output instead of tables
      --session ID      Resume an existing chat session (chat only); every
                        turn prints its session id, and interactive mode
                        prints a resume command on exit
      --user ID         User id for session scoping (chat only)
      --idt TOKEN       Internal Delegation Token sent as X-TRX-IDT on agent
                        chat/invoke/sessions calls (default: $TRX_IDT); tool
                        run does not use it
      --version         Print version and exit

Connection resolution — nats CLI contexts are first-class: with no options,
the selected default context is used (just like the nats CLI itself).
Explicit -s/--server or --context win; $NATS_URL applies only when no
context is selected. --creds/--nkey/--jwt+--seed override the context's
credentials.

Examples:
  nats-agents list
  nats-agents tools --json
  nats-agents chat copayAssistant "How many claims failed yesterday?"
  nats-agents chat copayAssistant                       (interactive)
  nats-agents chat copayAssistant --session 6a2f... "and the day before?"
  nats-agents run create_chart '{"chart":{"type":"bar","data":{"labels":["A"],"datasets":[{"data":[1]}]}}}'
`

type config struct {
	server  string
	context string
	creds   string
	nkey    string
	jwt     string
	seed    string
	timeout time.Duration
	asJSON  bool
	session string
	user    string
}

func main() {
	cfg := config{}
	showVersion := false

	fs := flag.NewFlagSet("nats-agents", flag.ExitOnError)
	fs.Usage = func() { fmt.Fprint(os.Stderr, usageText) }
	fs.StringVar(&cfg.server, "s", "", "")
	fs.StringVar(&cfg.server, "server", "", "")
	fs.StringVar(&cfg.context, "context", "", "")
	fs.StringVar(&cfg.creds, "creds", "", "")
	fs.StringVar(&cfg.nkey, "nkey", "", "")
	fs.StringVar(&cfg.jwt, "jwt", "", "")
	fs.StringVar(&cfg.seed, "seed", "", "")
	fs.DurationVar(&cfg.timeout, "timeout", 3*time.Second, "")
	fs.BoolVar(&cfg.asJSON, "json", false, "")
	fs.StringVar(&cfg.session, "session", "", "")
	fs.StringVar(&cfg.user, "user", "", "")
	idt := fs.String("idt", os.Getenv("TRX_IDT"), "Internal Delegation Token to send as X-TRX-IDT (default $TRX_IDT)")
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
	ctx := agentclient.WithIDT(context.Background(), *idt)

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
		requireArg(rest, 1, "chat <agent> [text]")
		// One more re-parse so flags may also follow the agent name
		// (`chat myAgent --session ID "text"`), same convention as
		// flags-after-command.
		agentName := rest[0]
		_ = fs.Parse(rest[1:])
		if text := fs.Args(); len(text) == 0 {
			chatInteractive(ctx, ac, agentName, cfg)
		} else {
			chat(ctx, ac, agentName, strings.Join(text, " "), cfg)
		}
	case "run":
		requireArg(rest, 2, "run <tool> <json-input>")
		runTool(ctx, tc, rest[0], rest[1], cfg)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", cmd)
		fs.Usage()
		os.Exit(2)
	}
}

// natsContext represents a NATS CLI context configuration
// (~/.config/nats/context/<name>.json) — same shape nats-discover reads.
type natsContext struct {
	URL         string `json:"url"`
	User        string `json:"user,omitempty"`
	Password    string `json:"password,omitempty"`
	Token       string `json:"token,omitempty"`
	Creds       string `json:"creds,omitempty"`
	NKey        string `json:"nkey,omitempty"`
	Cert        string `json:"cert,omitempty"`
	Key         string `json:"key,omitempty"`
	CA          string `json:"ca,omitempty"`
	JWT         string `json:"jwt,omitempty"`
	Seed        string `json:"seed,omitempty"`
	Description string `json:"description,omitempty"`
}

// connect resolves the server and credentials with nats CLI contexts as a
// first-class citizen: explicit flags win (-s / --context), otherwise the
// nats CLI's selected default context is used — running with no options at
// all behaves exactly like the nats CLI itself. $NATS_URL is the fallback
// when no context is selected (CI, containers, the bastion). Explicit
// credential flags override the context's credentials, and
// $NATS_JWT/$NATS_KEY fill in when nothing else supplied auth.
func connect(cfg config) (*nats.Conn, error) {
	var url string
	var opts []nats.Option
	haveAuth := false

	useContext := func(ctx *natsContext) {
		url = ctx.URL
		ctxOpts := contextToOptions(ctx)
		haveAuth = len(ctxOpts) > 0
		opts = append(opts, ctxOpts...)
	}

	switch {
	case cfg.server != "":
		url = cfg.server
	case cfg.context != "":
		ctx, err := loadNatsContext(cfg.context)
		if err != nil {
			return nil, fmt.Errorf("failed to load context %q: %w", cfg.context, err)
		}
		useContext(ctx)
	default:
		if ctx, err := loadDefaultContext(); err == nil {
			useContext(ctx)
		} else if envURL := os.Getenv("NATS_URL"); envURL != "" {
			url = envURL
		} else {
			return nil, fmt.Errorf("no NATS server specified: use -s, --context, select a nats CLI context, or set $NATS_URL (%v)", err)
		}
	}

	// Explicit credential flags override whatever the context provided.
	if cfg.creds != "" {
		opts = append(opts, nats.UserCredentials(cfg.creds))
		haveAuth = true
	}
	if cfg.nkey != "" {
		opt, err := nats.NkeyOptionFromSeed(cfg.nkey)
		if err != nil {
			return nil, fmt.Errorf("failed to load NKey: %w", err)
		}
		opts = append(opts, opt)
		haveAuth = true
	}
	jwt, seed := cfg.jwt, cfg.seed
	if jwt == "" && seed == "" && !haveAuth {
		jwt, seed = os.Getenv("NATS_JWT"), os.Getenv("NATS_KEY")
	}
	if jwt != "" && seed != "" {
		opts = append(opts, nats.UserJWTAndSeed(jwt, seed))
	}

	opts = append(opts, nats.Name("nats-agents-cli"))
	return nats.Connect(url, opts...)
}

// getNatsConfigDirs returns possible NATS config directories in order of
// preference: ~/.config/nats (where the nats CLI stores contexts), then
// os.UserConfigDir() as fallback (~/Library/Application Support on macOS).
func getNatsConfigDirs() []string {
	var dirs []string
	if home, err := os.UserHomeDir(); err == nil {
		dirs = append(dirs, filepath.Join(home, ".config", "nats"))
	}
	if configDir, err := os.UserConfigDir(); err == nil {
		dirs = append(dirs, filepath.Join(configDir, "nats"))
	}
	return dirs
}

func loadNatsContext(name string) (*natsContext, error) {
	for _, dir := range getNatsConfigDirs() {
		data, err := os.ReadFile(filepath.Join(dir, "context", name+".json"))
		if err != nil {
			continue
		}
		var ctx natsContext
		if err := json.Unmarshal(data, &ctx); err != nil {
			return nil, fmt.Errorf("invalid context file: %w", err)
		}
		return &ctx, nil
	}
	return nil, fmt.Errorf("context %q not found", name)
}

func loadDefaultContext() (*natsContext, error) {
	for _, dir := range getNatsConfigDirs() {
		data, err := os.ReadFile(filepath.Join(dir, "context.txt"))
		if err != nil {
			continue
		}
		name := strings.TrimSpace(string(data))
		if name == "" {
			continue
		}
		return loadNatsContext(name)
	}
	return nil, fmt.Errorf("no default context found")
}

func contextToOptions(ctx *natsContext) []nats.Option {
	var opts []nats.Option
	if ctx.User != "" && ctx.Password != "" {
		opts = append(opts, nats.UserInfo(ctx.User, ctx.Password))
	}
	if ctx.Token != "" {
		opts = append(opts, nats.Token(ctx.Token))
	}
	if ctx.Creds != "" {
		opts = append(opts, nats.UserCredentials(ctx.Creds))
	}
	if ctx.NKey != "" {
		if opt, err := nats.NkeyOptionFromSeed(ctx.NKey); err == nil {
			opts = append(opts, opt)
		}
	}
	if ctx.JWT != "" && ctx.Seed != "" {
		opts = append(opts, nats.UserJWTAndSeed(ctx.JWT, ctx.Seed))
	}
	if ctx.Cert != "" && ctx.Key != "" {
		opts = append(opts, nats.ClientCert(ctx.Cert, ctx.Key))
	}
	if ctx.CA != "" {
		opts = append(opts, nats.RootCAs(ctx.CA))
	}
	return opts
}

func chat(ctx context.Context, ac *agentclient.Client, agent, text string, cfg config) {
	_, err := chatTurn(ctx, ac, agent, text, cfg.session, cfg)
	if err != nil {
		fatal("[error] %v", err)
	}
}

// chatInteractive is a multi-turn REPL: every turn is sent with the session
// id from the previous ack (or --session to resume an old conversation), so
// the agent keeps the whole conversation's context. Exit with "exit",
// "quit", or Ctrl-D; a turn-level error is printed but keeps the session
// alive.
func chatInteractive(ctx context.Context, ac *agentclient.Client, agent string, cfg config) {
	sessionID := cfg.session
	if sessionID != "" {
		fmt.Fprintf(os.Stderr, "chatting with %s (resuming session %s) — exit, quit, or Ctrl-D to leave\n", agent, sessionID)
	} else {
		fmt.Fprintf(os.Stderr, "chatting with %s — exit, quit, or Ctrl-D to leave\n", agent)
	}

	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for {
		fmt.Fprintf(os.Stderr, "\n%s> ", agent)
		if !sc.Scan() {
			fmt.Fprintln(os.Stderr)
			break
		}
		text := strings.TrimSpace(sc.Text())
		if text == "" {
			continue
		}
		if text == "exit" || text == "quit" {
			break
		}
		sid, err := chatTurn(ctx, ac, agent, text, sessionID, cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[error] %v\n", err)
		}
		if sid != "" {
			sessionID = sid
		}
	}
	if sessionID != "" {
		fmt.Fprintf(os.Stderr, "resume this conversation with: nats-agents chat %s --session %s\n", agent, sessionID)
	}
}

// chatTurn runs one streaming turn and returns the session id from the ack
// so callers can chain turns. Run-level errors are returned, not fatal'd.
func chatTurn(ctx context.Context, ac *agentclient.Client, agent, text, sessionID string, cfg config) (string, error) {
	run, err := ac.Chat(ctx, agent, wire.ChatRequest{
		SessionID: sessionID,
		UserID:    cfg.user,
		Message:   wire.Message{Role: "user", Content: []wire.ContentBlock{{Text: text}}},
	})
	if err != nil {
		return sessionID, err
	}
	fmt.Fprintf(os.Stderr, "run %s session %s (instance %s)\n", run.Ack.RunID, run.Ack.SessionID, run.Ack.InstanceID)

	var runErr error
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
			input := compact(ev.ToolInput, 120)
			if input == "" || input == "null" {
				input = "{}"
			}
			fmt.Fprintf(os.Stderr, "[tool] %s %s\n", ev.ToolName, input)
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
			runErr = fmt.Errorf("%s", ev.Error)
		}
	}
	return run.Ack.SessionID, runErr
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
