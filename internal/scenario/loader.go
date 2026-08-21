package scenario

import (
	"embed"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"go.yaml.in/yaml/v3"
)

// bundledFS holds a copy of the repository's scenarios/ tree compiled into the binary.
// Keeping the copy inside the package is what makes a `go install`ed khaos usable with
// no checkout present.
//
//go:embed bundled
var bundledFS embed.FS

// bundledRoot is the directory prefix inside bundledFS.
const bundledRoot = "bundled"

// BundledPrefix marks a path that names a scenario compiled into the binary rather than
// a file on disk. Discover and Resolve return such paths when no on-disk scenarios
// directory is available, and Load understands them.
const BundledPrefix = "embed://"

// DefaultScenariosDir is the on-disk directory preferred over the bundled copy when it
// exists, resolved relative to the working directory.
const DefaultScenariosDir = "scenarios"

// Load reads, validates and decodes one scenario file.
//
// The returned error covers only I/O -- the file could not be read. Everything about
// the file's CONTENT, including a YAML syntax error, comes back as Diagnostics, so a
// caller that wants to report all problems at once has them in one place. The scenario
// is nil exactly when the diagnostics contain an error.
func Load(path string) (*Scenario, Diagnostics, error) {
	data, err := readScenarioFile(path)
	if err != nil {
		return nil, nil, err
	}
	sc, diags := Decode(data)
	return sc, diags, nil
}

func readScenarioFile(p string) ([]byte, error) {
	if name, ok := strings.CutPrefix(p, BundledPrefix); ok {
		data, err := bundledFS.ReadFile(path.Join(bundledRoot, name))
		if err != nil {
			return nil, fmt.Errorf("read bundled scenario %q: %w", name, err)
		}
		return data, nil
	}
	data, err := os.ReadFile(p)
	if err != nil {
		return nil, fmt.Errorf("read scenario file: %w", err)
	}
	return data, nil
}

// Discover finds every scenario reachable from roots and maps its name to its path.
//
// A scenario's name is its path relative to the root with the extension removed and
// forward slashes throughout -- "traffic/high-throughput", not the `name:` field inside
// the file. Two quirks are deliberate:
//
//   - only *.yaml is discovered. A .yml file is invisible to discovery even though
//     Resolve will happily load one by explicit path.
//   - a file that fails to parse, or that has no `name:` key, is skipped in silence.
//     Discovery must not fail because of one broken file.
//
// With no roots the on-disk DefaultScenariosDir is used when it exists and the bundled
// copy otherwise. With explicit roots, earlier roots win over later ones for a duplicate
// name. Returned on-disk paths are absolute; bundled paths carry BundledPrefix.
func Discover(roots ...string) (map[string]string, error) {
	if len(roots) == 0 {
		if info, err := os.Stat(DefaultScenariosDir); err == nil && info.IsDir() {
			roots = []string{DefaultScenariosDir}
		} else {
			return discoverBundled()
		}
	}

	found := make(map[string]string)
	for _, root := range roots {
		if info, err := os.Stat(root); err != nil || !info.IsDir() {
			continue // a missing root simply yields nothing
		}
		err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || filepath.Ext(p) != ".yaml" {
				return nil
			}
			data, err := os.ReadFile(p)
			if err != nil || !looksLikeScenario(data) {
				return nil
			}
			rel, err := filepath.Rel(root, p)
			if err != nil {
				return nil
			}
			key := scenarioKey(rel)
			if _, taken := found[key]; taken {
				return nil
			}
			abs, err := filepath.Abs(p)
			if err != nil {
				return nil
			}
			found[key] = abs
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("discover scenarios in %q: %w", root, err)
		}
	}
	return found, nil
}

func discoverBundled() (map[string]string, error) {
	found := make(map[string]string)
	err := fs.WalkDir(bundledFS, bundledRoot, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || path.Ext(p) != ".yaml" {
			return nil
		}
		rel := strings.TrimPrefix(p, bundledRoot+"/")
		data, err := bundledFS.ReadFile(p)
		if err != nil || !looksLikeScenario(data) {
			return nil
		}
		found[scenarioKey(rel)] = BundledPrefix + rel
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("discover bundled scenarios: %w", err)
	}
	return found, nil
}

// scenarioKey turns a relative file path into a scenario name.
func scenarioKey(rel string) string {
	rel = filepath.ToSlash(rel)
	return strings.TrimSuffix(rel, path.Ext(rel))
}

// looksLikeScenario is discovery's cheap admission test: parseable YAML mapping with a
// `name` key. It deliberately does NOT validate -- an invalid scenario is still listed,
// so that `khaos validate` can be pointed at it by name.
func looksLikeScenario(data []byte) bool {
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return false
	}
	return mapGet(&doc, "name") != nil
}

// Resolve turns a scenario name or file path into a path Load can read.
//
// Three cases:
//
//  1. a path ending in .yaml or .yml is used as given and must exist;
//  2. an absolute path, or one starting with ./ or ../, gets .yaml and then .yml
//     appended and the first that exists is used;
//  3. anything else is a bundled scenario name looked up through Discover.
//
// Case 2 REPLACES any existing extension rather than appending: "./release-1.2"
// resolves to "./release-1.yaml", not "./release-1.2.yaml". That is surprising, but
// changing it would silently load a different file than before for anyone relying on
// the current behaviour.
func Resolve(nameOrPath string, roots ...string) (string, error) {
	if strings.HasSuffix(nameOrPath, ".yaml") || strings.HasSuffix(nameOrPath, ".yml") {
		if !fileExists(nameOrPath) {
			return "", fmt.Errorf("scenario file not found: %q", nameOrPath)
		}
		return absOrSelf(nameOrPath), nil
	}

	if filepath.IsAbs(nameOrPath) || strings.HasPrefix(nameOrPath, "./") || strings.HasPrefix(nameOrPath, "../") {
		stem := strings.TrimSuffix(nameOrPath, filepath.Ext(nameOrPath))
		for _, ext := range []string{".yaml", ".yml"} {
			if candidate := stem + ext; fileExists(candidate) {
				return absOrSelf(candidate), nil
			}
		}
		return "", fmt.Errorf("scenario file not found: %q (tried .yaml and .yml)", nameOrPath)
	}

	available, err := Discover(roots...)
	if err != nil {
		return "", err
	}
	if p, ok := available[nameOrPath]; ok {
		return p, nil
	}
	names := make([]string, 0, len(available))
	for name := range available {
		names = append(names, name)
	}
	slices.Sort(names)
	return "", fmt.Errorf("unknown scenario: %q. Available: %s", nameOrPath, strings.Join(names, ", "))
}

func fileExists(p string) bool {
	info, err := os.Stat(p)
	return err == nil && !info.IsDir()
}

// absOrSelf makes a path absolute, falling back to the original if that is impossible.
func absOrSelf(p string) string {
	if abs, err := filepath.Abs(p); err == nil {
		return abs
	}
	return p
}
