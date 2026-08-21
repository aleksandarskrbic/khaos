package scenario

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// repoScenariosDir is the on-disk corpus, relative to this package.
const repoScenariosDir = "../../scenarios"

// TestShippedScenariosValidate pins that every scenario shipped in the repository
// decodes with zero errors.
func TestShippedScenariosValidate(t *testing.T) {
	files := yamlFilesUnder(t, repoScenariosDir)
	if len(files) < 20 {
		t.Fatalf("expected the shipped corpus to have at least 20 scenarios, found %d", len(files))
	}
	for _, file := range files {
		t.Run(filepath.Base(file), func(t *testing.T) {
			sc, diags, err := Load(file)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if diags.HasErrors() {
				t.Fatalf("shipped scenario does not validate:\n%s", diags.Errors())
			}
			if sc == nil {
				t.Fatal("no errors reported but scenario is nil")
			}
			if sc.Name == "" {
				t.Error("decoded scenario has an empty name")
			}
			if len(sc.Topics) == 0 && len(sc.Flows) == 0 {
				t.Error("decoded scenario has neither topics nor flows")
			}
		})
	}
}

// TestBundledCopyMatchesRepo pins that the embedded copy never drifts from the
// repository corpus it mirrors.
func TestBundledCopyMatchesRepo(t *testing.T) {
	repo := yamlFilesUnder(t, repoScenariosDir)
	bundled, err := discoverBundled()
	if err != nil {
		t.Fatalf("discoverBundled: %v", err)
	}
	if len(repo) != len(bundled) {
		t.Fatalf("repo has %d scenarios, bundled copy has %d", len(repo), len(bundled))
	}
	for _, file := range repo {
		rel, err := filepath.Rel(repoScenariosDir, file)
		if err != nil {
			t.Fatal(err)
		}
		key := scenarioKey(rel)
		embedded, ok := bundled[key]
		if !ok {
			t.Errorf("%s is missing from the bundled copy", key)
			continue
		}
		want, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		got, err := readScenarioFile(embedded)
		if err != nil {
			t.Fatalf("read bundled %s: %v", key, err)
		}
		if string(got) != string(want) {
			t.Errorf("bundled copy of %s differs from scenarios/%s", key, rel)
		}
	}
}

func TestDiscover(t *testing.T) {
	root := t.TempDir()
	write(t, filepath.Join(root, "traffic", "high-throughput.yaml"), "name: high-throughput\ntopics: [{name: a}]\n")
	write(t, filepath.Join(root, "chaos", "nested", "deep.yaml"), "name: deep\ntopics: [{name: a}]\n")
	// Discovery only looks at *.yaml, so this one stays invisible.
	write(t, filepath.Join(root, "ignored.yml"), "name: ignored\ntopics: [{name: a}]\n")
	// Unparseable and name-less files are skipped in silence.
	write(t, filepath.Join(root, "broken.yaml"), "name: [unterminated\n")
	write(t, filepath.Join(root, "nameless.yaml"), "topics: [{name: a}]\n")

	found, err := Discover(root)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	want := []string{"chaos/nested/deep", "traffic/high-throughput"}
	if len(found) != len(want) {
		t.Fatalf("found %v, want keys %v", found, want)
	}
	for _, key := range want {
		p, ok := found[key]
		if !ok {
			t.Fatalf("missing key %q in %v", key, found)
		}
		if !filepath.IsAbs(p) {
			t.Errorf("%q maps to %q, want an absolute path", key, p)
		}
	}
}

func TestDiscoverMissingRootIsEmpty(t *testing.T) {
	found, err := Discover(filepath.Join(t.TempDir(), "does-not-exist"))
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(found) != 0 {
		t.Fatalf("want no scenarios, got %v", found)
	}
}

func TestDiscoverEarlierRootWins(t *testing.T) {
	first, second := t.TempDir(), t.TempDir()
	write(t, filepath.Join(first, "a.yaml"), "name: from-first\ntopics: [{name: a}]\n")
	write(t, filepath.Join(second, "a.yaml"), "name: from-second\ntopics: [{name: a}]\n")
	write(t, filepath.Join(second, "b.yaml"), "name: only-in-second\ntopics: [{name: a}]\n")

	found, err := Discover(first, second)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if !strings.HasPrefix(found["a"], first) {
		t.Errorf("a resolved to %q, want it under the first root %q", found["a"], first)
	}
	if !strings.HasPrefix(found["b"], second) {
		t.Errorf("b resolved to %q, want it under the second root %q", found["b"], second)
	}
}

// TestDiscoverBundledFallback exercises the no-roots path from a working directory that
// has no scenarios/ dir, which is how an installed binary always runs.
func TestDiscoverBundledFallback(t *testing.T) {
	chdir(t, t.TempDir())
	found, err := Discover()
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(found) == 0 {
		t.Fatal("expected the bundled copy to be discovered")
	}
	p, ok := found["traffic/high-throughput"]
	if !ok {
		t.Fatalf("traffic/high-throughput missing from %v", found)
	}
	if !strings.HasPrefix(p, BundledPrefix) {
		t.Errorf("bundled scenario resolved to %q, want a %s path", p, BundledPrefix)
	}
	sc, diags, err := Load(p)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if diags.HasErrors() {
		t.Fatalf("bundled scenario does not validate:\n%s", diags.Errors())
	}
	if sc.Name != "high-throughput" {
		t.Errorf("Name = %q, want high-throughput", sc.Name)
	}
}

// TestDiscoverPrefersOnDiskDir pins the on-disk-over-bundled preference.
func TestDiscoverPrefersOnDiskDir(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, DefaultScenariosDir, "local.yaml"), "name: local\ntopics: [{name: a}]\n")
	chdir(t, dir)

	found, err := Discover()
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(found) != 1 {
		t.Fatalf("want only the on-disk scenario, got %v", found)
	}
	if _, ok := found["local"]; !ok {
		t.Errorf("want key %q, got %v", "local", found)
	}
}

func TestResolve(t *testing.T) {
	root := tempDir(t)
	write(t, filepath.Join(root, "traffic", "high-throughput.yaml"), "name: high-throughput\ntopics: [{name: a}]\n")
	write(t, filepath.Join(root, "explicit.yml"), "name: explicit\ntopics: [{name: a}]\n")
	chdir(t, root)

	tests := []struct {
		name    string
		input   string
		want    string
		wantErr string
	}{
		{
			name:  "explicit .yaml path",
			input: filepath.Join(root, "traffic", "high-throughput.yaml"),
			want:  filepath.Join(root, "traffic", "high-throughput.yaml"),
		},
		{
			// Resolve loads a .yml by explicit path even though Discover ignores it.
			name:  "explicit .yml path",
			input: "./explicit.yml",
			want:  filepath.Join(root, "explicit.yml"),
		},
		{
			name:  "relative path without extension tries .yaml",
			input: "./traffic/high-throughput",
			want:  filepath.Join(root, "traffic", "high-throughput.yaml"),
		},
		{
			name:  "relative path without extension falls back to .yml",
			input: "./explicit",
			want:  filepath.Join(root, "explicit.yml"),
		},
		{
			name:  "absolute path without extension",
			input: filepath.Join(root, "traffic", "high-throughput"),
			want:  filepath.Join(root, "traffic", "high-throughput.yaml"),
		},
		{
			name:  "bundled name lookup",
			input: "chaos/broker-chaos",
			want:  BundledPrefix + "chaos/broker-chaos.yaml",
		},
		{
			name:    "missing explicit file",
			input:   "./nope.yaml",
			wantErr: "scenario file not found",
		},
		{
			name:    "missing extensionless path",
			input:   "./nope",
			wantErr: "tried .yaml and .yml",
		},
		{
			name:    "unknown bundled name lists alternatives",
			input:   "nope",
			wantErr: "unknown scenario",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Resolve(tt.input)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("Resolve(%q) = %q, want error containing %q", tt.input, got, tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want it to contain %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Resolve(%q): %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("Resolve(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// TestResolveReplacesExistingExtension pins that an extensionless path containing a dot
// loses everything after the last one, per the quirk documented on Resolve.
func TestResolveReplacesExistingExtension(t *testing.T) {
	root := tempDir(t)
	write(t, filepath.Join(root, "release-1.yaml"), "name: release\ntopics: [{name: a}]\n")
	chdir(t, root)

	got, err := Resolve("./release-1.2")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if want := filepath.Join(root, "release-1.yaml"); got != want {
		t.Errorf("Resolve(./release-1.2) = %q, want %q", got, want)
	}
}

func TestLoadMissingFile(t *testing.T) {
	_, _, err := Load(filepath.Join(t.TempDir(), "absent.yaml"))
	if err == nil {
		t.Fatal("want an error for a missing file")
	}
}

// TestLoadInvalidYAMLIsDiagnostic pins that a broken file comes back as a diagnostic
// rather than a Go error.
func TestLoadInvalidYAMLIsDiagnostic(t *testing.T) {
	p := filepath.Join(t.TempDir(), "broken.yaml")
	write(t, p, "name: [unterminated\n")

	sc, diags, err := Load(p)
	if err != nil {
		t.Fatalf("Load returned a Go error for a syntax problem: %v", err)
	}
	if sc != nil {
		t.Error("want a nil scenario for unparseable YAML")
	}
	if !diags.HasErrors() {
		t.Fatal("want an error diagnostic")
	}
	if got := diags[0].Path; got != "yaml" {
		t.Errorf("diagnostic path = %q, want %q", got, "yaml")
	}
	if !strings.Contains(diags[0].Message, "Invalid YAML syntax") {
		t.Errorf("message = %q, want it to mention invalid YAML syntax", diags[0].Message)
	}
}

func yamlFilesUnder(t *testing.T, dir string) []string {
	t.Helper()
	var files []string
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && filepath.Ext(p) == ".yaml" {
			files = append(files, p)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
	return files
}

// tempDir is t.TempDir with symlinks resolved. Resolve returns absolute paths, and on
// macOS the temp dir is reached through a /var -> /private/var symlink, so a raw
// t.TempDir would never compare equal to what Resolve returns.
func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func write(t *testing.T, p, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// chdir switches the working directory for one test. Discover's no-roots path is
// defined relative to the working directory, so exercising it requires this.
func chdir(t *testing.T, dir string) {
	t.Helper()
	previous, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(previous); err != nil {
			t.Fatal(err)
		}
	})
}
