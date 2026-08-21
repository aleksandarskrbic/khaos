package localcluster

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestMaterializeAssetsWritesAllFilesOnce(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "compose")

	written, err := materializeAssets(dir)
	if err != nil {
		t.Fatalf("first materialize: %v", err)
	}
	sort.Strings(written)
	want := []string{composeKRaft, composeSRKRaft, composeSRZK, composeZooKeeper}
	sort.Strings(want)
	if !reflect.DeepEqual(written, want) {
		t.Fatalf("first materialize wrote %v, want %v", written, want)
	}

	// Idempotent: identical content must not be rewritten, because `docker compose`
	// may be reading the file concurrently and Up/Down must see a stable project.
	written, err = materializeAssets(dir)
	if err != nil {
		t.Fatalf("second materialize: %v", err)
	}
	if len(written) != 0 {
		t.Fatalf("second materialize rewrote %v, want no writes", written)
	}
}

// Idempotence has to hold on disk, not just in the returned list: a `docker compose up`
// in another process may have the file open, and rewriting identical bytes underneath it
// is a needless risk. Modification times must be untouched by a repeat call.
func TestMaterializeAssetsDoesNotTouchUnchangedFilesOnDisk(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "compose")
	if _, err := materializeAssets(dir); err != nil {
		t.Fatalf("first materialize: %v", err)
	}

	before := modTimes(t, dir)
	time.Sleep(20 * time.Millisecond) // so a rewrite would produce a different mtime

	for i := range 3 {
		written, err := materializeAssets(dir)
		if err != nil {
			t.Fatalf("materialize %d: %v", i+2, err)
		}
		if len(written) != 0 {
			t.Fatalf("materialize %d rewrote %v", i+2, written)
		}
	}

	if after := modTimes(t, dir); !reflect.DeepEqual(before, after) {
		t.Fatalf("compose files were rewritten:\nbefore %v\nafter  %v", before, after)
	}
}

func modTimes(t *testing.T, dir string) map[string]time.Time {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	out := make(map[string]time.Time, len(entries))
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			t.Fatalf("stat %s: %v", e.Name(), err)
		}
		out[e.Name()] = info.ModTime()
	}
	return out
}

func TestMaterializeAssetsRepairsDivergedFile(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "compose")
	if _, err := materializeAssets(dir); err != nil {
		t.Fatalf("materialize: %v", err)
	}

	path := filepath.Join(dir, composeKRaft)
	if err := os.WriteFile(path, []byte("tampered\n"), 0o644); err != nil {
		t.Fatalf("tamper: %v", err)
	}

	written, err := materializeAssets(dir)
	if err != nil {
		t.Fatalf("re-materialize: %v", err)
	}
	if !reflect.DeepEqual(written, []string{composeKRaft}) {
		t.Fatalf("rewrote %v, want only %s", written, composeKRaft)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read repaired file: %v", err)
	}
	want, err := assetsFS.ReadFile("assets/" + composeKRaft)
	if err != nil {
		t.Fatalf("read embedded: %v", err)
	}
	if string(got) != string(want) {
		t.Fatal("repaired file does not match the embedded copy")
	}
}

func TestComposeDirIsStable(t *testing.T) {
	first, err := composeDir()
	if err != nil {
		t.Fatalf("composeDir: %v", err)
	}
	second, err := composeDir()
	if err != nil {
		t.Fatalf("composeDir: %v", err)
	}
	if first != second {
		t.Fatalf("composeDir is not stable: %q then %q", first, second)
	}
	if filepath.Base(first) != "compose" || filepath.Base(filepath.Dir(first)) != "khaos" {
		t.Fatalf("composeDir = %q, want .../khaos/compose", first)
	}
}

// TestNewMaterializesIntoTheStableDir exercises the real constructor. It writes into the
// user cache dir, which is exactly what a khaos run does, so there is nothing to isolate.
func TestNewMaterializesIntoTheStableDir(t *testing.T) {
	c, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if !c.kraft {
		t.Error("KRaft must be the default mode")
	}
	if c.schemaRegistry {
		t.Error("Schema Registry must be off by default")
	}

	want, err := composeDir()
	if err != nil {
		t.Fatalf("composeDir: %v", err)
	}
	if c.dir != want {
		t.Fatalf("dir = %q, want %q", c.dir, want)
	}
	if _, err := os.Stat(c.clusterFile()); err != nil {
		t.Fatalf("cluster compose file not materialized: %v", err)
	}
	if _, err := os.Stat(c.registryFile()); err != nil {
		t.Fatalf("registry compose file not materialized: %v", err)
	}

	zk, err := New(WithKRaft(false), WithSchemaRegistry(true))
	if err != nil {
		t.Fatalf("New(zookeeper): %v", err)
	}
	if filepath.Base(zk.clusterFile()) != composeZooKeeper {
		t.Errorf("WithKRaft(false) selected %q", zk.clusterFile())
	}
	if !zk.schemaRegistry {
		t.Error("WithSchemaRegistry(true) had no effect")
	}
}

// TestEmbeddedNetworkMatchesProject pins the network name every compose file must agree
// on: "<ProjectName>_kafka-net", not a name that only resolves when the compose files
// happen to live in a directory called docker/.
func TestEmbeddedNetworkMatchesProject(t *testing.T) {
	for _, name := range []string{composeSRKRaft, composeSRZK} {
		t.Run(name, func(t *testing.T) {
			data, err := assetsFS.ReadFile("assets/" + name)
			if err != nil {
				t.Fatalf("read embedded: %v", err)
			}
			body := string(data)
			if !strings.Contains(body, "name: "+NetworkName) {
				t.Errorf("missing external network %q", NetworkName)
			}
			if strings.Contains(body, "docker_kafka-net") {
				t.Error("still references the pre-fix network name docker_kafka-net")
			}
		})
	}
	if NetworkName != ProjectName+"_kafka-net" {
		t.Fatalf("NetworkName %q does not derive from ProjectName %q", NetworkName, ProjectName)
	}
}
