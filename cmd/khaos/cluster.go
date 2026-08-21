package main

import (
	"fmt"
	"strings"

	"charm.land/lipgloss/v2"
	"charm.land/lipgloss/v2/table"
	"github.com/aleksandarskrbic/khaos/internal/localcluster"
	"github.com/spf13/cobra"
)

// clusterFlags are the options shared by the cluster commands; they select which
// compose files get used.
type clusterFlags struct {
	mode           string
	schemaRegistry bool
}

func (f *clusterFlags) bind(c *cobra.Command) {
	// --mode/-m defaults to kraft, matching the spelling `run` already uses; cluster-up
	// used to take a --kraft boolean instead, which matched neither the README nor its
	// own sibling command.
	c.Flags().StringVarP(&f.mode, "mode", "m", "kraft", "cluster mode: kraft or zookeeper")
}

// bindSchemaRegistry is cluster-up only. cluster-down and cluster-status do not take it:
// Down tears the registry down whenever its container is running, and Status does not
// look at it, so offering the flag there would advertise an option that does nothing.
func (f *clusterFlags) bindSchemaRegistry(c *cobra.Command) {
	c.Flags().BoolVar(&f.schemaRegistry, "schema-registry", false, "also start Schema Registry")
}

func (f *clusterFlags) cluster() (*localcluster.Cluster, error) {
	kraft, err := parseMode(f.mode)
	if err != nil {
		return nil, err
	}
	return localcluster.New(
		localcluster.WithKRaft(kraft),
		localcluster.WithSchemaRegistry(f.schemaRegistry),
	)
}

func newClusterUpCmd() *cobra.Command {
	var f clusterFlags
	cmd := &cobra.Command{
		Use:   "cluster-up",
		Short: "Start the local Kafka cluster",
		Long: "Start the bundled three-broker Kafka cluster with Docker Compose.\n\n" +
			"The compose files are embedded in the binary and written to a stable directory\n" +
			"under the user cache, so `cluster-up` and `cluster-down` always address the same\n" +
			"compose project regardless of the working directory.",
		Args: usageArgs(cobra.NoArgs),
		RunE: func(c *cobra.Command, _ []string) error {
			cl, err := f.cluster()
			if err != nil {
				return err
			}
			err = spin(c.OutOrStdout(), "Starting Kafka cluster",
				"(a first run pulls ~1GB of images and can take minutes)",
				func() error { return cl.Up(c.Context()) })
			if err != nil {
				return err
			}
			brokers, err := cl.BootstrapServers(c.Context())
			if err != nil {
				return err
			}
			spinDone(c.OutOrStdout(), "Kafka cluster ready", brokers)
			return nil
		},
	}
	f.bind(cmd)
	f.bindSchemaRegistry(cmd)
	return cmd
}

func newClusterDownCmd() *cobra.Command {
	var (
		f       clusterFlags
		volumes bool
	)
	cmd := &cobra.Command{
		Use:   "cluster-down",
		Short: "Stop the local Kafka cluster and remove its volumes",
		Long: "Stop the bundled Kafka cluster.\n\n" +
			"Pass --volumes to remove data volumes as well. Note that the bundled compose\n" +
			"files declare no volumes -- the brokers keep their logs inside the container --\n" +
			"so today the next `cluster-up` starts empty either way.",
		Args: usageArgs(cobra.NoArgs),
		RunE: func(c *cobra.Command, _ []string) error {
			cl, err := f.cluster()
			if err != nil {
				return err
			}
			// --volumes/-v: volumes are kept unless asked for.
			err = spin(c.OutOrStdout(), "Stopping Kafka cluster", "",
				func() error { return cl.Down(c.Context(), volumes) })
			if err != nil {
				return err
			}
			spinDone(c.OutOrStdout(), "Kafka cluster stopped", "")
			return nil
		},
	}
	cmd.Flags().BoolVarP(&volumes, "volumes", "v", false, "also remove data volumes")
	f.bind(cmd)
	return cmd
}

func newClusterStatusCmd() *cobra.Command {
	var f clusterFlags
	cmd := &cobra.Command{
		Use:   "cluster-status",
		Short: "Show the local cluster's services",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(c *cobra.Command, _ []string) error {
			cl, err := f.cluster()
			if err != nil {
				return err
			}
			services, err := cl.Status(c.Context())
			if err != nil {
				return err
			}
			if len(services) == 0 {
				// Goes to stderr so `khaos cluster-status | wc -l` counts services, not
				// prose.
				fmt.Fprintln(c.ErrOrStderr(),
					"no Kafka containers found; run `khaos cluster-up` first")
				return nil
			}

			t := table.New().
				Border(lipgloss.RoundedBorder()).
				BorderStyle(styBorder).
				BorderRow(false).
				BorderColumn(false).
				StyleFunc(func(row, col int) lipgloss.Style {
					pad := lipgloss.NewStyle().PaddingLeft(1).PaddingRight(2)
					if row == table.HeaderRow {
						return pad.Inherit(styHeader)
					}
					return pad
				}).
				Headers("SERVICE", "STATE", "PORT")

			for _, s := range services {
				port := ""
				if s.PublishedPort > 0 {
					port = fmt.Sprintf("localhost:%d", s.PublishedPort)
				}
				// State is the one column worth colouring: it is why you ran this.
				state := s.State
				if strings.Contains(strings.ToLower(state), "running") {
					state = styOK.Render(state)
				} else {
					state = styWarn.Render(state)
				}
				t.Row(s.Service, state, styDim.Render(port))
			}
			fmt.Fprintln(c.OutOrStdout(), t)
			return nil
		},
	}
	f.bind(cmd)
	return cmd
}
