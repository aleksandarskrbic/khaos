package localcluster

import (
	"context"
	"slices"
	"testing"
)

// Data volumes are kept unless --volumes is passed. For the bundled stack this changes
// nothing observable -- none of the four compose files declares a volume -- but the
// argument must be right so it stays correct if one is added.
func TestDownPassesVolumesFlagOnlyWhenAsked(t *testing.T) {
	tests := []struct {
		name          string
		removeVolumes bool
		wantDashV     bool
	}{
		{"default keeps volumes", false, false},
		{"--volumes removes them", true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeRunner()
			c := newTestCluster(t, f)

			if err := c.Down(context.Background(), tt.removeVolumes); err != nil {
				t.Fatalf("Down: %v", err)
			}

			var sawDown bool
			for _, call := range f.calls {
				if subcommand(call) != "down" {
					continue
				}
				sawDown = true
				if got := slices.Contains(call, "-v"); got != tt.wantDashV {
					t.Errorf("`docker %v`: -v present = %v, want %v", call, got, tt.wantDashV)
				}
			}
			if !sawDown {
				t.Fatal("no compose down call recorded")
			}
		})
	}
}

// Every compose invocation must carry the explicit project name. Without it compose infers
// the project from the compose file's parent directory, which is what made Schema Registry
// unusable from an installed binary.
func TestDownAlwaysNamesTheProject(t *testing.T) {
	f := newFakeRunner()
	c := newTestCluster(t, f, WithSchemaRegistry(true))

	if err := c.Down(context.Background(), true); err != nil {
		t.Fatalf("Down: %v", err)
	}
	for _, call := range f.calls {
		if !slices.Contains(call, "-p") || !slices.Contains(call, ProjectName) {
			t.Errorf("`docker %v` is missing -p %s", call, ProjectName)
		}
	}
}
