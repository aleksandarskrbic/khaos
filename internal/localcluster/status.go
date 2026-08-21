package localcluster

import (
	"encoding/json"
	"sort"
	"strings"
)

// ServiceStatus is one row of `docker compose ps`. The port is kept numeric rather than
// pre-rendered into a URL, since formatting it is a presentation concern for the caller.
type ServiceStatus struct {
	Service       string
	State         string
	PublishedPort int
}

// Broker client ports published by the bundled compose files.
//
// assets/docker-compose.kraft.yml:9,41,73 (and the zk variant) map kafka-1/2/3 to
// 9092/9093/9094, and each broker additionally publishes its JMX port whose CONTAINER
// side is always 9101 (assets/docker-compose.kraft.yml:10,42,74).
const (
	jmxTargetPort      = 9101
	defaultKafkaPort   = 9092
	kafkaUIServiceName = "kafka-ui"
)

var kafkaClientPorts = map[int]bool{9092: true, 9093: true, 9094: true}

// composePublisher is the subset of compose's ps JSON publisher object we need.
type composePublisher struct {
	URL           string `json:"URL"`
	TargetPort    int    `json:"TargetPort"`
	PublishedPort int    `json:"PublishedPort"`
	Protocol      string `json:"Protocol"`
}

type composePS struct {
	Service    string             `json:"Service"`
	Name       string             `json:"Name"`
	State      string             `json:"State"`
	Publishers []composePublisher `json:"Publishers"`
}

// parsePS decodes `docker compose ps --format json` output.
//
// Compose emits one JSON object per line (older versions emitted a single array; both are
// accepted here). Unparseable lines are skipped rather than failing the call, because a
// partially-started project routinely produces noise on stdout and a status view should
// still render.
//
// Rows are returned sorted by service name so callers -- notably BootstrapServers -- get a
// stable broker ordering instead of whatever order compose happened to print.
func parsePS(stdout string) []ServiceStatus {
	trimmed := strings.TrimSpace(stdout)
	if trimmed == "" {
		return nil
	}

	var rows []composePS
	if strings.HasPrefix(trimmed, "[") {
		if err := json.Unmarshal([]byte(trimmed), &rows); err != nil {
			return nil
		}
	} else {
		for _, line := range strings.Split(trimmed, "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			var row composePS
			if err := json.Unmarshal([]byte(line), &row); err != nil {
				continue
			}
			rows = append(rows, row)
		}
	}

	out := make([]ServiceStatus, 0, len(rows))
	for _, row := range rows {
		service := row.Service
		if service == "" {
			service = row.Name
		}
		if service == "" {
			service = "unknown"
		}
		state := row.State
		if state == "" {
			state = "unknown"
		}
		out = append(out, ServiceStatus{
			Service:       service,
			State:         state,
			PublishedPort: selectPublishedPort(service, row.Publishers),
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Service < out[j].Service })
	return out
}

// selectPublishedPort picks the port a client should connect to, deliberately.
//
// Every broker publishes two ports -- its Kafka listener and its JMX listener -- and
// compose does not promise an order among Publishers, so picking the first one risks
// handing out the JMX port as a bootstrap server. The choice is instead made by
// container-side port: brokers advertise their client listener on one of kafkaClientPorts
// and JMX always on jmxTargetPort. Non-broker services publish a single port, and the
// lowest published port is chosen so the result stays deterministic.
func selectPublishedPort(service string, pubs []composePublisher) int {
	isBroker := strings.HasPrefix(service, "kafka-") && service != kafkaUIServiceName

	best := 0
	for _, p := range pubs {
		if p.PublishedPort <= 0 {
			continue
		}
		if isBroker {
			if p.TargetPort == jmxTargetPort || !kafkaClientPorts[p.TargetPort] {
				continue
			}
			return p.PublishedPort
		}
		if best == 0 || p.PublishedPort < best {
			best = p.PublishedPort
		}
	}

	if isBroker {
		// No publisher advertised a known client port. Fall back to the lowest
		// non-JMX published port rather than reporting nothing.
		for _, p := range pubs {
			if p.PublishedPort <= 0 || p.TargetPort == jmxTargetPort {
				continue
			}
			if best == 0 || p.PublishedPort < best {
				best = p.PublishedPort
			}
		}
	}
	return best
}

// brokerAddresses renders the bootstrap list from a status snapshot: broker services are
// those named "kafka-*" other than kafka-ui, sorted by service name, addressed as
// 127.0.0.1 to dodge IPv6-first resolution, falling back to 127.0.0.1:9092 when nothing
// is published.
func brokerAddresses(statuses []ServiceStatus) string {
	var brokers []string
	for _, s := range statuses {
		if !strings.HasPrefix(s.Service, "kafka-") || s.Service == kafkaUIServiceName {
			continue
		}
		if s.PublishedPort <= 0 {
			continue
		}
		brokers = append(brokers, hostPort(s.PublishedPort))
	}
	if len(brokers) == 0 {
		return hostPort(defaultKafkaPort)
	}
	return strings.Join(brokers, ",")
}
