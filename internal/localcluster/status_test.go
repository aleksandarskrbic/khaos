package localcluster

import (
	"context"
	"reflect"
	"testing"
)

// A kafka-1 row exactly as compose emits it, with the JMX publisher listed FIRST: naively
// taking the first publisher with a PublishedPort would hand out 9101 here.
const jmxFirstRow = `{"Service":"kafka-1","Name":"kafka-1","State":"running","Publishers":[` +
	`{"URL":"0.0.0.0","TargetPort":9101,"PublishedPort":9101,"Protocol":"tcp"},` +
	`{"URL":"0.0.0.0","TargetPort":9092,"PublishedPort":9092,"Protocol":"tcp"}]}`

func TestSelectPublishedPort(t *testing.T) {
	tests := []struct {
		name    string
		service string
		pubs    []composePublisher
		want    int
	}{
		{
			name:    "broker skips the JMX publisher listed first",
			service: "kafka-1",
			pubs: []composePublisher{
				{TargetPort: 9101, PublishedPort: 9101},
				{TargetPort: 9092, PublishedPort: 9092},
			},
			want: 9092,
		},
		{
			name:    "broker with remapped JMX publisher",
			service: "kafka-2",
			pubs: []composePublisher{
				{TargetPort: 9101, PublishedPort: 9102},
				{TargetPort: 9093, PublishedPort: 9093},
			},
			want: 9093,
		},
		{
			name:    "broker falls back to the lowest non-JMX port",
			service: "kafka-9",
			pubs: []composePublisher{
				{TargetPort: 9101, PublishedPort: 9109},
				{TargetPort: 19099, PublishedPort: 19099},
				{TargetPort: 19098, PublishedPort: 19098},
			},
			want: 19098,
		},
		{
			name:    "kafka-ui is not a broker and keeps its single port",
			service: "kafka-ui",
			pubs:    []composePublisher{{TargetPort: 8080, PublishedPort: 8080}},
			want:    8080,
		},
		{
			name:    "schema registry",
			service: "schema-registry",
			pubs:    []composePublisher{{TargetPort: 8081, PublishedPort: 8081}},
			want:    8081,
		},
		{
			name:    "unpublished service",
			service: "kafka-3",
			pubs:    []composePublisher{{TargetPort: 9094, PublishedPort: 0}},
			want:    0,
		},
		{
			name:    "no publishers at all",
			service: "zookeeper",
			want:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := selectPublishedPort(tt.service, tt.pubs); got != tt.want {
				t.Fatalf("selectPublishedPort = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestParsePS(t *testing.T) {
	tests := []struct {
		name   string
		stdout string
		want   []ServiceStatus
	}{
		{
			name:   "empty output",
			stdout: "  \n",
			want:   nil,
		},
		{
			name: "jsonl, sorted by service",
			stdout: `{"Service":"kafka-ui","State":"running","Publishers":[{"TargetPort":8080,"PublishedPort":8080}]}` + "\n" +
				jmxFirstRow + "\n",
			want: []ServiceStatus{
				{Service: "kafka-1", State: "running", PublishedPort: 9092},
				{Service: "kafka-ui", State: "running", PublishedPort: 8080},
			},
		},
		{
			name:   "json array form",
			stdout: `[{"Service":"zookeeper","State":"exited","Publishers":[]}]`,
			want:   []ServiceStatus{{Service: "zookeeper", State: "exited", PublishedPort: 0}},
		},
		{
			name:   "falls back to Name and unknown state",
			stdout: `{"Name":"kafka-1","Publishers":[]}`,
			want:   []ServiceStatus{{Service: "kafka-1", State: "unknown", PublishedPort: 0}},
		},
		{
			// Skipping an unparseable line keeps the rest of the batch usable.
			name:   "unparseable line is skipped",
			stdout: "not json\n" + `{"Service":"kafka-1","State":"running","Publishers":[]}`,
			want:   []ServiceStatus{{Service: "kafka-1", State: "running", PublishedPort: 0}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parsePS(tt.stdout); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("parsePS = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestBrokerAddresses(t *testing.T) {
	tests := []struct {
		name     string
		statuses []ServiceStatus
		want     string
	}{
		{
			name: "three brokers, ui and registry excluded",
			statuses: []ServiceStatus{
				{Service: "kafka-1", PublishedPort: 9092},
				{Service: "kafka-2", PublishedPort: 9093},
				{Service: "kafka-3", PublishedPort: 9094},
				{Service: "kafka-ui", PublishedPort: 8080},
				{Service: "schema-registry", PublishedPort: 8081},
			},
			want: "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094",
		},
		{
			name:     "unpublished brokers are dropped",
			statuses: []ServiceStatus{{Service: "kafka-1", PublishedPort: 0}, {Service: "kafka-2", PublishedPort: 9093}},
			want:     "127.0.0.1:9093",
		},
		{
			name:     "nothing published falls back to the conventional port",
			statuses: nil,
			want:     "127.0.0.1:9092",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := brokerAddresses(tt.statuses); got != tt.want {
				t.Fatalf("brokerAddresses = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestBootstrapServersAvoidsJMXPort is the end-to-end version of the port-selection fix.
func TestBootstrapServersAvoidsJMXPort(t *testing.T) {
	f := newFakeRunner()
	f.stdout["ps"] = jmxFirstRow
	c := newTestCluster(t, f)

	got, err := c.BootstrapServers(context.Background())
	if err != nil {
		t.Fatalf("BootstrapServers: %v", err)
	}
	if got != "127.0.0.1:9092" {
		t.Fatalf("BootstrapServers = %q, want the Kafka listener, not the JMX port", got)
	}
}

func TestStatusTolerantOfUnstartedProject(t *testing.T) {
	f := newFakeRunner()
	f.fail["ps"] = true
	f.stderr["ps"] = "no configuration file provided"
	c := newTestCluster(t, f)

	got, err := c.Status(context.Background())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if got != nil {
		t.Fatalf("Status = %+v, want nil", got)
	}
}

func TestStatusSurfacesDaemonFailure(t *testing.T) {
	f := newFakeRunner()
	f.fail["ps"] = true
	f.stderr["ps"] = "Cannot connect to the Docker daemon at unix:///var/run/docker.sock."
	c := newTestCluster(t, f)

	if _, err := c.Status(context.Background()); err == nil {
		t.Fatal("want an error when docker itself is unreachable")
	}
}
