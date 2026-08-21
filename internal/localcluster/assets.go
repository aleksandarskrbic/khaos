package localcluster

import (
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// Compose file names for the four bundled stacks.
const (
	composeKRaft     = "docker-compose.kraft.yml"
	composeZooKeeper = "docker-compose.zk.yml"
	composeSRKRaft   = "docker-compose.schema-registry.kraft.yml"
	composeSRZK      = "docker-compose.schema-registry.zk.yml"
)

// assetsFS carries the four bundled compose files inside the binary, so there is exactly
// one copy and no separate asset directory to locate at runtime.
//
//go:embed assets/*.yml
var assetsFS embed.FS

// composeDir returns the stable on-disk directory the embedded compose files are
// materialized into.
//
// `docker compose -f` needs a real path that stays the same across invocations, since Up
// and Down must address the same project and a per-invocation temp dir would break the
// bind between the cluster's network and the Schema Registry's external network reference.
func composeDir() (string, error) {
	cache, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("locate user cache dir: %w", err)
	}
	return filepath.Join(cache, "khaos", "compose"), nil
}

// materializeAssets writes every embedded compose file into dir, creating it if needed.
//
// A file is rewritten only when its content differs from the embedded copy, so a running
// `docker compose` never sees its file change underneath it and repeated calls are cheap.
// It returns the names of the files it actually wrote, which is what the tests assert on.
func materializeAssets(dir string) ([]string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create compose dir %s: %w", dir, err)
	}

	entries, err := fs.ReadDir(assetsFS, "assets")
	if err != nil {
		return nil, fmt.Errorf("read embedded assets: %w", err)
	}

	var written []string
	for _, e := range entries {
		want, err := assetsFS.ReadFile("assets/" + e.Name())
		if err != nil {
			return nil, fmt.Errorf("read embedded asset %s: %w", e.Name(), err)
		}

		path := filepath.Join(dir, e.Name())
		got, err := os.ReadFile(path)
		switch {
		case err == nil && string(got) == string(want):
			continue
		case err != nil && !errors.Is(err, fs.ErrNotExist):
			return nil, fmt.Errorf("read existing compose file %s: %w", path, err)
		}

		if err := os.WriteFile(path, want, 0o644); err != nil {
			return nil, fmt.Errorf("write compose file %s: %w", path, err)
		}
		written = append(written, e.Name())
	}
	return written, nil
}
