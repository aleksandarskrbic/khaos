package localcluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// errFakeExit stands in for exec's non-zero-exit error.
var errFakeExit = errors.New("exit status 1")

// fakeRunner records every docker invocation and replays canned results, so the whole
// command surface is testable with no docker daemon anywhere in sight.
type fakeRunner struct {
	calls  [][]string
	stdout map[string]string // keyed by the compose subcommand ("ps", "up", ...)
	stderr map[string]string
	fail   map[string]bool
}

func newFakeRunner() *fakeRunner {
	return &fakeRunner{
		stdout: map[string]string{},
		stderr: map[string]string{},
		fail:   map[string]bool{},
	}
}

// subcommand extracts the verb from a docker argv: "compose -p khaos -f FILE ps ..."
// yields "ps", and a bare "ps --filter ..." yields "docker-ps".
func subcommand(args []string) string {
	if len(args) > 0 && args[0] != "compose" {
		return "docker-" + args[0]
	}
	if len(args) > 5 {
		return args[5]
	}
	return ""
}

func (f *fakeRunner) run(_ context.Context, args ...string) (string, string, error) {
	f.calls = append(f.calls, append([]string(nil), args...))
	key := subcommand(args)
	if f.fail[key] {
		return f.stdout[key], f.stderr[key], errFakeExit
	}
	return f.stdout[key], f.stderr[key], nil
}

func (f *fakeRunner) callFor(sub string) []string {
	for _, c := range f.calls {
		if subcommand(c) == sub {
			return c
		}
	}
	return nil
}

func newTestCluster(t *testing.T, f *fakeRunner, opts ...Option) *Cluster {
	t.Helper()
	c := &Cluster{kraft: true, dir: "/cache/khaos/compose", run: f.run}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// TestArgumentConstruction pins the exact argv for every command, most importantly the
// explicit -p flag on every compose invocation.
func TestArgumentConstruction(t *testing.T) {
	const file = "/cache/khaos/compose/docker-compose.kraft.yml"

	tests := []struct {
		name string
		got  []string
		want []string
	}{
		{
			name: "up",
			got:  upArgs(file),
			want: []string{"compose", "-p", "khaos", "-f", file, "up", "-d"},
		},
		{
			name: "down with volumes",
			got:  downArgs(file, true),
			want: []string{"compose", "-p", "khaos", "-f", file, "down", "-v"},
		},
		{
			name: "down keeping volumes",
			got:  downArgs(file, false),
			want: []string{"compose", "-p", "khaos", "-f", file, "down"},
		},
		{
			name: "ps",
			got:  psArgs(file),
			want: []string{"compose", "-p", "khaos", "-f", file, "ps", "--format", "json"},
		},
		{
			name: "stop service",
			got:  stopArgs(file, "kafka-2"),
			want: []string{"compose", "-p", "khaos", "-f", file, "stop", "kafka-2"},
		},
		{
			name: "start service",
			got:  startArgs(file, "kafka-2"),
			want: []string{"compose", "-p", "khaos", "-f", file, "start", "kafka-2"},
		},
		{
			name: "docker ps name filter",
			got:  containerRunningArgs("schema-registry"),
			want: []string{"ps", "--filter", "name=schema-registry", "--format", "{{.Names}}"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !reflect.DeepEqual(tt.got, tt.want) {
				t.Fatalf("args = %v, want %v", tt.got, tt.want)
			}
		})
	}
}

// TestEveryComposeInvocationCarriesProject is the belt-and-braces version of the fix: no
// matter which entry point is exercised, -p must be present.
func TestEveryComposeInvocationCarriesProject(t *testing.T) {
	f := newFakeRunner()
	f.stdout["ps"] = `{"Service":"kafka-1","State":"running","Publishers":[{"TargetPort":9092,"PublishedPort":9092}]}`
	c := newTestCluster(t, f, WithSchemaRegistry(true))

	ctx := context.Background()
	if _, err := c.Status(ctx); err != nil {
		t.Fatalf("Status: %v", err)
	}
	if err := c.StopBroker(ctx, "kafka-1"); err != nil {
		t.Fatalf("StopBroker: %v", err)
	}
	if err := c.StartBroker(ctx, "kafka-1"); err != nil {
		t.Fatalf("StartBroker: %v", err)
	}
	if err := c.Down(ctx, false); err != nil {
		t.Fatalf("Down: %v", err)
	}

	if len(f.calls) == 0 {
		t.Fatal("no docker calls recorded")
	}
	for _, call := range f.calls {
		if call[0] != "compose" {
			continue
		}
		if len(call) < 3 || call[1] != "-p" || call[2] != ProjectName {
			t.Errorf("compose call without -p %s: %v", ProjectName, call)
		}
	}
}

func TestComposeFileSelectionByMode(t *testing.T) {
	tests := []struct {
		name         string
		kraft        bool
		wantCluster  string
		wantRegistry string
	}{
		{"kraft", true, composeKRaft, composeSRKRaft},
		{"zookeeper", false, composeZooKeeper, composeSRZK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Cluster{dir: "/d", kraft: tt.kraft}
			if got := c.clusterFile(); got != "/d/"+tt.wantCluster {
				t.Errorf("clusterFile = %q, want /d/%s", got, tt.wantCluster)
			}
			if got := c.registryFile(); got != "/d/"+tt.wantRegistry {
				t.Errorf("registryFile = %q, want /d/%s", got, tt.wantRegistry)
			}
		})
	}
}

func TestDownTearsDownRegistryBeforeCluster(t *testing.T) {
	f := newFakeRunner()
	c := newTestCluster(t, f, WithSchemaRegistry(true))

	if err := c.Down(context.Background(), false); err != nil {
		t.Fatalf("Down: %v", err)
	}

	var downFiles []string
	for _, call := range f.calls {
		if subcommand(call) == "down" {
			downFiles = append(downFiles, call[4])
		}
	}
	want := []string{"/cache/khaos/compose/" + composeSRKRaft, "/cache/khaos/compose/" + composeKRaft}
	if !reflect.DeepEqual(downFiles, want) {
		t.Fatalf("down order = %v, want %v", downFiles, want)
	}
}

// A registry teardown failure must not stop the brokers from coming down.
func TestDownIgnoresRegistryFailure(t *testing.T) {
	f := newFakeRunner()
	f.fail["down"] = true
	f.stderr["down"] = "no such service: schema-registry"
	c := newTestCluster(t, f, WithSchemaRegistry(true))

	err := c.Down(context.Background(), false)
	if err == nil {
		t.Fatal("want the cluster teardown failure to surface")
	}
	if !strings.Contains(err.Error(), "stop containers") {
		t.Fatalf("error came from the wrong stack: %v", err)
	}
	if got := len(f.calls); got != 2 {
		t.Fatalf("made %d calls, want both teardowns attempted", got)
	}
}

func TestBrokerActionRequiresRunningCluster(t *testing.T) {
	f := newFakeRunner() // ps returns nothing -> project has no services
	c := newTestCluster(t, f)

	err := c.StopBroker(context.Background(), "kafka-1")
	if !errors.Is(err, ErrNoActiveCluster) {
		t.Fatalf("err = %v, want ErrNoActiveCluster", err)
	}
}

func TestUpSurfacesClassifiedComposeError(t *testing.T) {
	f := newFakeRunner()
	f.fail["up"] = true
	f.stderr["up"] = "Cannot connect to the Docker daemon at unix:///var/run/docker.sock."
	c := newTestCluster(t, f)

	err := c.Up(context.Background())
	if !errors.Is(err, ErrDockerNotRunning) {
		t.Fatalf("err = %v, want ErrDockerNotRunning", err)
	}
	if len(f.calls) != 1 {
		t.Fatalf("made %d calls, want to stop after the failed up", len(f.calls))
	}
}

// Up is the one entry point the -p audit above cannot reach, because it polls for broker
// readiness. A stub listener stands in for the broker so the whole path runs in
// milliseconds with no docker and no Kafka.
func TestUpPassesProjectAndWaitsForTheAdvertisedPort(t *testing.T) {
	port := listeningPort(t)

	f := newFakeRunner()
	f.stdout["ps"] = fmt.Sprintf(
		`{"Service":"kafka-1","State":"running","Publishers":[{"TargetPort":9101,"PublishedPort":%d},{"TargetPort":9092,"PublishedPort":%d}]}`,
		port+1, port)
	c := newTestCluster(t, f)

	if err := c.Up(context.Background()); err != nil {
		t.Fatalf("Up: %v", err)
	}

	up := f.callFor("up")
	if up == nil {
		t.Fatal("Up never invoked `compose up`")
	}
	want := []string{"compose", "-p", ProjectName, "-f", c.clusterFile(), "up", "-d"}
	if !reflect.DeepEqual(up, want) {
		t.Fatalf("up args = %v, want %v", up, want)
	}
	if f.callFor("ps") == nil {
		t.Error("Up did not consult `compose ps` for the bootstrap servers")
	}
}

// The Schema Registry stack is only brought up when it is not already running, and when
// it is brought up the invocation still carries -p, which is what makes the network name
// resolve.
func TestUpSkipsSchemaRegistryWhenAlreadyRunning(t *testing.T) {
	port := listeningPort(t)
	psJSON := fmt.Sprintf(`{"Service":"kafka-1","State":"running","Publishers":[{"TargetPort":9092,"PublishedPort":%d}]}`, port)

	tests := []struct {
		name        string
		dockerPS    string
		wantUpFiles []string
	}{
		{
			name:        "registry already up",
			dockerPS:    "khaos-schema-registry-1\n",
			wantUpFiles: nil,
		},
		{
			name:        "registry absent",
			dockerPS:    "",
			wantUpFiles: []string{composeSRKRaft},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeRunner()
			f.stdout["ps"] = psJSON
			f.stdout["docker-ps"] = tt.dockerPS
			c := newTestCluster(t, f, WithSchemaRegistry(true))

			// waitForSchemaRegistry would poll a real HTTP endpoint for 60s, so stop
			// after the decision this test is about has been made.
			_ = c.Up(cancelledAfterSchemaRegistryDecision(t))

			var gotUpFiles []string
			for _, call := range f.calls {
				if subcommand(call) != "up" {
					continue
				}
				if base := filepath.Base(call[4]); base != composeKRaft {
					gotUpFiles = append(gotUpFiles, base)
				}
				if call[1] != "-p" || call[2] != ProjectName {
					t.Errorf("up call without -p %s: %v", ProjectName, call)
				}
			}
			if !reflect.DeepEqual(gotUpFiles, tt.wantUpFiles) {
				t.Fatalf("registry up calls = %v, want %v", gotUpFiles, tt.wantUpFiles)
			}
		})
	}
}

// cancelledAfterSchemaRegistryDecision returns a context that is alive long enough for Up
// to reach (and act on) the Schema Registry branch and then cancels, so the readiness poll
// returns immediately instead of waiting out its 60s budget.
func cancelledAfterSchemaRegistryDecision(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	t.Cleanup(cancel)
	return ctx
}

// listeningPort returns a port with a protocol-answering fake broker attached for the
// lifetime of the test. Up's readiness probe demands a real ApiVersions answer -- a bare
// accept-and-close listener is precisely the docker-proxy behaviour the probe exists to
// reject, so a test double that only accepts would hang the poll.
func listeningPort(t *testing.T) int {
	t.Helper()
	ln := fakeBroker(t)
	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().(*net.TCPAddr).Port
}

func TestIsContainerRunning(t *testing.T) {
	tests := []struct {
		name   string
		stdout string
		fail   bool
		want   bool
	}{
		{"present", "schema-registry\n", false, true},
		{"absent", "kafka-1\nkafka-2\n", false, false},
		{"docker unavailable", "", true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeRunner()
			f.stdout["docker-ps"] = tt.stdout
			f.fail["docker-ps"] = tt.fail
			c := newTestCluster(t, f)

			if got := c.isContainerRunning(context.Background(), "schema-registry"); got != tt.want {
				t.Fatalf("isContainerRunning = %v, want %v", got, tt.want)
			}
		})
	}
}
