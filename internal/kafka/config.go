// Package kafka builds the franz-go clients khaos runs its traffic through, and the
// admin helpers that prepare the topics those clients use.
//
// franz-go's defaults do not always match the client behaviour khaos wants to reproduce, so
// each deliberate departure from franz-go's default is documented and centralized in
// policy.go.
//
// This package builds clients and nothing else. Producing, consuming, rate limiting,
// failure injection and statistics belong to internal/engine; the contract here is a
// correctly configured *kgo.Client.
package kafka

import (
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Config is everything needed to reach a cluster.
type Config struct {
	// BootstrapServers is the seed broker list.
	BootstrapServers []string

	Security Security

	// External marks a cluster khaos does not own -- one reached via
	// `khaos simulate --bootstrap-servers ...` rather than the bundled Docker compose
	// cluster.
	//
	// retention.ms is set on created topics only for the local cluster, because an
	// external cluster may enforce retention policies that reject the override. This
	// cannot be derived from Security: an external plaintext cluster and a local one have
	// identical Security. See EnsureTopics.
	External bool
}

// Validate checks the configuration without contacting anything.
func (c Config) Validate() error {
	if len(c.BootstrapServers) == 0 {
		return errors.New("no bootstrap servers configured")
	}
	for _, b := range c.BootstrapServers {
		if b == "" {
			return errors.New("bootstrap server list contains an empty entry")
		}
	}
	return c.Security.Validate()
}

// baseOptions returns the options common to producers, consumers and the admin client.
// franz-go logs nothing unless kgo.WithLogger is passed, so silence is the default; the
// engine can attach a slog-backed logger when it wants one.
func (c Config) baseOptions() ([]kgo.Opt, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	secOpts, err := c.Security.Options()
	if err != nil {
		return nil, fmt.Errorf("build security options: %w", err)
	}

	opts := make([]kgo.Opt, 0, len(secOpts)+1)
	opts = append(opts, kgo.SeedBrokers(c.BootstrapServers...))
	opts = append(opts, secOpts...)
	return opts, nil
}
