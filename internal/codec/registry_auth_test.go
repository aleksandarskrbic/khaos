package codec

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/srfake"
)

// basicHeader is the Authorization header franz-go produces for BasicAuth, which is what
// http.Request.SetBasicAuth produces.
func basicHeader(user, pass string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
}

// authRecorder captures the Authorization header of every request reaching a fake
// registry, so a test can assert what was actually sent rather than what was configured.
type authRecorder struct {
	mu      sync.Mutex
	headers []string
}

func (a *authRecorder) install(fake *srfake.Registry) {
	fake.Intercept(func(_ http.ResponseWriter, r *http.Request) bool {
		a.mu.Lock()
		a.headers = append(a.headers, r.Header.Get("Authorization"))
		a.mu.Unlock()
		return false
	})
}

func (a *authRecorder) seen() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.headers...)
}

// The credentials must reach the registry on the wire, and -- the point of DEFAULTS
// UNCHANGED -- a config with only a URL must send no Authorization header at all.
func TestRegistryAuthSendsHeader(t *testing.T) {
	tests := []struct {
		name   string
		expect string // what the fake registry demands; "" disables its auth middleware
		cfg    func(url string) RegistryConfig
		want   string
	}{
		{
			name: "no credentials sends nothing",
			cfg:  func(url string) RegistryConfig { return RegistryConfig{URL: url} },
			want: "",
		},
		{
			name:   "basic auth",
			expect: basicHeader("SRAPIKEY", "sr-api-secret"),
			cfg: func(url string) RegistryConfig {
				return RegistryConfig{URL: url, Username: "SRAPIKEY", Password: "sr-api-secret"}
			},
			want: basicHeader("SRAPIKEY", "sr-api-secret"),
		},
		{
			name:   "bearer token",
			expect: "Bearer tok-123",
			cfg: func(url string) RegistryConfig {
				return RegistryConfig{URL: url, BearerToken: "tok-123"}
			},
			want: "Bearer tok-123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var opts []srfake.Option
			if tt.expect != "" {
				opts = append(opts, srfake.WithAuth(tt.expect))
			}
			fake := srfake.New(opts...)
			t.Cleanup(fake.Close)

			var rec authRecorder
			rec.install(fake)

			// Construction runs the reachability probe, so a rejected credential fails
			// here rather than on the first produce.
			if _, err := NewRegistryWithConfig(tt.cfg(fake.URL())); err != nil {
				t.Fatalf("NewRegistryWithConfig: %v", err)
			}

			seen := rec.seen()
			if len(seen) == 0 {
				t.Fatal("the registry received no request")
			}
			for _, h := range seen {
				if h != tt.want {
					t.Errorf("Authorization = %q, want %q", h, tt.want)
				}
			}
		})
	}
}

// Credentials must keep working past construction: the probe, the subject fetch and the
// registration all go through the same client.
func TestRegistryAuthAppliesToEveryRequest(t *testing.T) {
	fake := srfake.New(srfake.WithAuth("Bearer tok-123"))
	t.Cleanup(fake.Close)
	fake.SeedSchema("orders-value", 1, 7, sr.Schema{Schema: testAvroSubjectSchema, Type: sr.TypeAvro})

	reg, err := NewRegistryWithConfig(RegistryConfig{URL: fake.URL(), BearerToken: "tok-123"})
	if err != nil {
		t.Fatalf("NewRegistryWithConfig: %v", err)
	}
	if _, _, err := reg.FieldsFor(context.Background(), "orders-value"); err != nil {
		t.Fatalf("FieldsFor: %v", err)
	}
}

// A rejected request must say it was rejected, name the registry, and point at a flag.
// Without this a Confluent Cloud user sees only `401 Unauthorized` and has no way to know
// that the Schema Registry API key is a different credential from the Kafka one.
func TestRegistryAuthErrorIsDiagnosable(t *testing.T) {
	tests := []struct {
		name string
		cfg  func(url string) RegistryConfig
		want []string
	}{
		{
			name: "no credentials at all",
			cfg:  func(url string) RegistryConfig { return RegistryConfig{URL: url} },
			want: []string{"401", "requires authentication", "--schema-registry-username", "--schema-registry-token"},
		},
		{
			name: "wrong basic auth",
			cfg: func(url string) RegistryConfig {
				return RegistryConfig{URL: url, Username: "SRAPIKEY", Password: "wrong"}
			},
			// 403: the fake answers Forbidden when the header is present but wrong.
			want: []string{"403", "--schema-registry-username", "NOT the Kafka cluster API key"},
		},
		{
			name: "wrong bearer token",
			cfg: func(url string) RegistryConfig {
				return RegistryConfig{URL: url, BearerToken: "stale"}
			},
			want: []string{"403", "--schema-registry-token"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := srfake.New(srfake.WithAuth(basicHeader("SRAPIKEY", "sr-api-secret")))
			t.Cleanup(fake.Close)

			_, err := NewRegistryWithConfig(tt.cfg(fake.URL()))
			if err == nil {
				t.Fatal("NewRegistryWithConfig succeeded against a registry that rejects it")
			}
			msg := err.Error()
			if !strings.Contains(msg, fake.URL()) {
				t.Errorf("error does not name the registry URL %q: %v", fake.URL(), msg)
			}
			for _, want := range tt.want {
				if !strings.Contains(msg, want) {
					t.Errorf("error is missing %q: %v", want, msg)
				}
			}
		})
	}
}

// The same treatment must apply after construction, or a subject-scoped ACL failure still
// reads as "schema not found".
func TestRegistryAuthErrorOnFetch(t *testing.T) {
	fake := srfake.New()
	t.Cleanup(fake.Close)

	// Let the startup probe through, then reject everything else, as a registry with
	// read-only-on-some-subjects ACLs would.
	var probed bool
	var mu sync.Mutex
	fake.Intercept(func(w http.ResponseWriter, _ *http.Request) bool {
		mu.Lock()
		defer mu.Unlock()
		if !probed {
			probed = true
			return false
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error_code":40301,"message":"User is denied operation Read on Subject"}`))
		return true
	})

	reg, err := NewRegistryWithConfig(RegistryConfig{URL: fake.URL(), Username: "u", Password: "p"})
	if err != nil {
		t.Fatalf("NewRegistryWithConfig: %v", err)
	}

	_, _, err = reg.FieldsFor(context.Background(), "orders-value")
	if err == nil {
		t.Fatal("FieldsFor succeeded against a registry that denies it")
	}
	for _, want := range []string{"403", `subject "orders-value"`, "--schema-registry-username"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error is missing %q: %v", want, err)
		}
	}
	if strings.Contains(err.Error(), "schema not found") {
		t.Errorf("a denied subject is reported as missing: %v", err)
	}
}

// Every one of these is a mistake that would otherwise surface as an opaque 401 or as a
// connection that silently is not encrypted, so all of them must fail at construction.
func TestRegistryConfigValidate(t *testing.T) {
	tests := []struct {
		name string
		cfg  RegistryConfig
		want string // substring; "" means the config must be accepted
	}{
		{
			name: "bare url",
			cfg:  RegistryConfig{URL: "http://localhost:8081"},
		},
		{
			name: "basic auth",
			cfg:  RegistryConfig{URL: "http://localhost:8081", Username: "u", Password: "p"},
		},
		{
			name: "bearer token",
			cfg:  RegistryConfig{URL: "http://localhost:8081", BearerToken: "t"},
		},
		{
			name: "empty url",
			cfg:  RegistryConfig{},
			want: "url is empty",
		},
		{
			name: "basic and bearer together",
			cfg:  RegistryConfig{URL: "http://localhost:8081", Username: "u", Password: "p", BearerToken: "t"},
			want: "cannot be combined",
		},
		{
			name: "bearer with only a username",
			cfg:  RegistryConfig{URL: "http://localhost:8081", Username: "u", BearerToken: "t"},
			want: "cannot be combined",
		},
		{
			name: "username without password",
			cfg:  RegistryConfig{URL: "http://localhost:8081", Username: "u"},
			want: "--schema-registry-password is required",
		},
		{
			name: "password without username",
			cfg:  RegistryConfig{URL: "http://localhost:8081", Password: "p"},
			want: "--schema-registry-username is required",
		},
		{
			name: "cert without key",
			cfg:  RegistryConfig{URL: "https://localhost:8081", CertLocation: "/c.pem"},
			want: "--schema-registry-key-location is required",
		},
		{
			name: "key without cert",
			cfg:  RegistryConfig{URL: "https://localhost:8081", KeyLocation: "/k.pem"},
			want: "--schema-registry-cert-location is required",
		},
		{
			name: "tls material against an http url",
			cfg:  RegistryConfig{URL: "http://localhost:8081", CALocation: "/ca.pem"},
			want: "is not https",
		},
		{
			name: "tls material against a schemeless url",
			cfg:  RegistryConfig{URL: "localhost:8081", CALocation: "/ca.pem"},
			want: "is not https",
		},
		{
			name: "tls material against an https url",
			cfg:  RegistryConfig{URL: "HTTPS://localhost:8081", CALocation: "/ca.pem"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			switch {
			case tt.want == "" && err != nil:
				t.Fatalf("Validate: %v, want nil", err)
			case tt.want == "":
				return
			case err == nil:
				t.Fatalf("Validate succeeded, want an error containing %q", tt.want)
			case !strings.Contains(err.Error(), tt.want):
				t.Errorf("Validate = %v, want an error containing %q", err, tt.want)
			}
		})
	}
}

// A rejected combination must never reach the network, so nothing that Validate refuses
// may produce a client.
func TestNewRegistryWithConfigRejectsBadCombinations(t *testing.T) {
	fake := srfake.New()
	t.Cleanup(fake.Close)

	tests := []struct {
		name string
		cfg  RegistryConfig
	}{
		{"basic and bearer", RegistryConfig{URL: fake.URL(), Username: "u", Password: "p", BearerToken: "t"}},
		{"username alone", RegistryConfig{URL: fake.URL(), Username: "u"}},
		{"password alone", RegistryConfig{URL: fake.URL(), Password: "p"}},
		{"cert alone", RegistryConfig{URL: fake.URL(), CertLocation: "/c.pem"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewRegistryWithConfig(tt.cfg); err == nil {
				t.Error("NewRegistryWithConfig accepted an incoherent credential set")
			}
		})
	}
}

func TestRegistryTLSConfigFromPEMFiles(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()
	caFile := writePEM(t, dir, "ca.pem", ca.certPEM)
	certPEM, keyPEM := ca.issue(t, "client")
	certFile := writePEM(t, dir, "client.pem", certPEM)
	keyFile := writePEM(t, dir, "client.key", keyPEM)
	junk := writePEM(t, dir, "junk.pem", []byte("this is not a certificate\n"))

	t.Run("ca only", func(t *testing.T) {
		cfg, err := RegistryConfig{URL: "https://sr", CALocation: caFile}.tlsConfig()
		if err != nil {
			t.Fatalf("tlsConfig: %v", err)
		}
		if cfg.RootCAs == nil {
			t.Error("RootCAs was not populated from the CA file")
		}
		if len(cfg.Certificates) != 0 {
			t.Error("a client certificate appeared without one being configured")
		}
	})

	t.Run("mutual tls", func(t *testing.T) {
		cfg, err := RegistryConfig{
			URL: "https://sr", CALocation: caFile, CertLocation: certFile, KeyLocation: keyFile,
		}.tlsConfig()
		if err != nil {
			t.Fatalf("tlsConfig: %v", err)
		}
		if cfg.RootCAs == nil {
			t.Error("RootCAs was not populated from the CA file")
		}
		if len(cfg.Certificates) != 1 {
			t.Fatalf("Certificates has %d entries, want 1", len(cfg.Certificates))
		}
	})

	t.Run("errors", func(t *testing.T) {
		errTests := []struct {
			name string
			cfg  RegistryConfig
			want string
		}{
			{
				name: "missing ca file",
				cfg:  RegistryConfig{URL: "https://sr", CALocation: filepath.Join(dir, "nope.pem")},
				want: "read schema registry CA certificate",
			},
			{
				name: "ca file with no certificates",
				cfg:  RegistryConfig{URL: "https://sr", CALocation: junk},
				want: "contains no valid PEM certificates",
			},
			{
				name: "mismatched key",
				cfg:  RegistryConfig{URL: "https://sr", CertLocation: certFile, KeyLocation: junk},
				want: "load schema registry client key pair",
			},
		}
		for _, tt := range errTests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := tt.cfg.tlsConfig()
				if err == nil {
					t.Fatalf("tlsConfig succeeded, want an error containing %q", tt.want)
				}
				if !strings.Contains(err.Error(), tt.want) {
					t.Errorf("tlsConfig = %v, want an error containing %q", err, tt.want)
				}
			})
		}
	})
}

// End to end over real TLS: the CA file must actually be what lets the probe succeed, and
// a client certificate must actually be presented.
func TestRegistryOverTLS(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()
	caFile := writePEM(t, dir, "ca.pem", ca.certPEM)

	serverPEM, serverKeyPEM := ca.issue(t, "127.0.0.1")
	serverCert, err := tls.X509KeyPair(serverPEM, serverKeyPEM)
	if err != nil {
		t.Fatalf("server key pair: %v", err)
	}
	clientPEM, clientKeyPEM := ca.issue(t, "client")
	clientCertFile := writePEM(t, dir, "client.pem", clientPEM)
	clientKeyFile := writePEM(t, dir, "client.key", clientKeyPEM)

	tests := []struct {
		name       string
		clientAuth tls.ClientAuthType
		cfg        func(url string) RegistryConfig
		wantErr    string // "" means the connection must succeed
	}{
		{
			name: "ca file trusts the server",
			cfg:  func(url string) RegistryConfig { return RegistryConfig{URL: url, CALocation: caFile} },
		},
		{
			name:    "without the ca file the server is untrusted",
			cfg:     func(url string) RegistryConfig { return RegistryConfig{URL: url} },
			wantErr: "unreachable",
		},
		{
			name:       "client certificate is presented for mtls",
			clientAuth: tls.RequireAndVerifyClientCert,
			cfg: func(url string) RegistryConfig {
				return RegistryConfig{
					URL: url, CALocation: caFile,
					CertLocation: clientCertFile, KeyLocation: clientKeyFile,
				}
			},
		},
		{
			name:       "mtls without a client certificate is refused",
			clientAuth: tls.RequireAndVerifyClientCert,
			cfg:        func(url string) RegistryConfig { return RegistryConfig{URL: url, CALocation: caFile} },
			wantErr:    "unreachable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := srfake.NewRegistry() // no server of its own; served over TLS below
			srv := httptest.NewUnstartedServer(fake.Handler())
			srv.TLS = &tls.Config{
				MinVersion:   tls.VersionTLS12,
				Certificates: []tls.Certificate{serverCert},
				ClientAuth:   tt.clientAuth,
				ClientCAs:    ca.pool(t),
			}
			// The refusal cases make the server log a handshake error, which is expected
			// here and only noise in the test output.
			srv.Config.ErrorLog = log.New(io.Discard, "", 0)
			srv.StartTLS()
			t.Cleanup(srv.Close)

			_, err := NewRegistryWithConfig(tt.cfg(srv.URL))
			switch {
			case tt.wantErr == "" && err != nil:
				t.Fatalf("NewRegistryWithConfig: %v", err)
			case tt.wantErr == "":
				return
			case err == nil:
				t.Fatal("NewRegistryWithConfig succeeded, want a TLS failure")
			case !strings.Contains(err.Error(), tt.wantErr):
				t.Errorf("error = %v, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

// testCA is a throwaway certificate authority: tests need real PEM files, and shipping
// fixtures that expire is worse than minting them per run.
type testCA struct {
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	certPEM []byte
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "khaos test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("self-sign CA: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA: %v", err)
	}
	return &testCA{
		cert:    cert,
		key:     key,
		certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	}
}

// issue signs a leaf certificate. A name that parses as an IP becomes a SAN IP, which is
// what a certificate for an httptest server on 127.0.0.1 needs.
func (ca *testCA) issue(t *testing.T, name string) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	if ip := net.ParseIP(name); ip != nil {
		tmpl.IPAddresses = []net.IP{ip}
	} else {
		tmpl.DNSNames = []string{name}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("sign leaf: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal leaf key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}

func (ca *testCA) pool(t *testing.T) *x509.CertPool {
	t.Helper()
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca.certPEM) {
		t.Fatal("the generated CA is not valid PEM")
	}
	return pool
}

func writePEM(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}
