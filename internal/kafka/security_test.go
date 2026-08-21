package kafka

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSecurityValidate(t *testing.T) {
	t.Parallel()

	// Coupling rules under test: protocol/mechanism enum membership and the SASL/mTLS
	// field pairing.
	tests := []struct {
		name    string
		sec     Security
		wantErr string // substring; empty means the config must validate
	}{
		{
			name: "zero value is plaintext",
			sec:  Security{},
		},
		{
			name: "plaintext",
			sec:  Security{Protocol: ProtocolPlaintext},
		},
		{
			name: "uppercase protocol from the CLI flag",
			sec:  Security{Protocol: "SASL_SSL", SASLMechanism: "PLAIN", SASLUsername: "u", SASLPassword: "p"},
		},
		{
			name: "ssl needs no credentials",
			sec:  Security{Protocol: ProtocolSSL},
		},
		{
			name: "sasl_plaintext with scram256",
			sec: Security{Protocol: ProtocolSASLPlaintext, SASLMechanism: MechanismScramSHA256,
				SASLUsername: "u", SASLPassword: "p"},
		},
		{
			name: "sasl_ssl with scram512",
			sec: Security{Protocol: ProtocolSASLSSL, SASLMechanism: MechanismScramSHA512,
				SASLUsername: "u", SASLPassword: "p"},
		},
		{
			name:    "unknown protocol",
			sec:     Security{Protocol: "kerberos"},
			wantErr: "invalid security protocol",
		},
		{
			name: "unknown mechanism",
			sec: Security{Protocol: ProtocolSASLSSL, SASLMechanism: "GSSAPI",
				SASLUsername: "u", SASLPassword: "p"},
			wantErr: "invalid SASL mechanism",
		},
		{
			name:    "sasl without mechanism",
			sec:     Security{Protocol: ProtocolSASLSSL, SASLUsername: "u", SASLPassword: "p"},
			wantErr: "sasl_mechanism required",
		},
		{
			name:    "sasl without username",
			sec:     Security{Protocol: ProtocolSASLSSL, SASLMechanism: MechanismPlain, SASLPassword: "p"},
			wantErr: "sasl_username and sasl_password required",
		},
		{
			name:    "sasl without password",
			sec:     Security{Protocol: ProtocolSASLSSL, SASLMechanism: MechanismPlain, SASLUsername: "u"},
			wantErr: "sasl_username and sasl_password required",
		},
		{
			name:    "cert without key",
			sec:     Security{Protocol: ProtocolSSL, CertLocation: "/tmp/c.pem"},
			wantErr: "ssl_key_location required",
		},
		{
			name:    "key without cert",
			sec:     Security{Protocol: ProtocolSSL, KeyLocation: "/tmp/k.pem"},
			wantErr: "ssl_cert_location required",
		},
		{
			// The message must name the flag; an obscure PEM parse failure from deep
			// inside crypto/tls is exactly what this check exists to prevent.
			name: "encrypted key is rejected by name",
			sec: Security{Protocol: ProtocolSSL, CertLocation: "/tmp/c.pem",
				KeyLocation: "/tmp/k.pem", KeyPassword: "hunter2"},
			wantErr: "--ssl-key-password: encrypted private keys are not supported",
		},
		{
			// A mechanism on a non-SASL protocol is inert (librdkafka ignores it
			// without SASL) and must not be an error.
			name: "mechanism without a sasl protocol is tolerated",
			sec:  Security{Protocol: ProtocolPlaintext, SASLMechanism: MechanismPlain},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.sec.Validate()
			switch {
			case tt.wantErr == "" && err != nil:
				t.Fatalf("Validate() = %v, want nil", err)
			case tt.wantErr != "" && err == nil:
				t.Fatalf("Validate() = nil, want error containing %q", tt.wantErr)
			case tt.wantErr != "" && !strings.Contains(err.Error(), tt.wantErr):
				t.Fatalf("Validate() = %q, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestSecurityOptions(t *testing.T) {
	t.Parallel()

	// One option per active feature: SASL adds kgo.SASL, TLS adds kgo.DialTLSConfig.
	tests := []struct {
		name     string
		sec      Security
		wantOpts int
	}{
		{name: "plaintext has nothing to configure", sec: Security{}, wantOpts: 0},
		{name: "ssl adds only tls", sec: Security{Protocol: ProtocolSSL}, wantOpts: 1},
		{
			name: "sasl_plaintext adds only sasl",
			sec: Security{Protocol: ProtocolSASLPlaintext, SASLMechanism: MechanismPlain,
				SASLUsername: "u", SASLPassword: "p"},
			wantOpts: 1,
		},
		{
			name: "sasl_ssl adds both",
			sec: Security{Protocol: ProtocolSASLSSL, SASLMechanism: MechanismScramSHA512,
				SASLUsername: "u", SASLPassword: "p"},
			wantOpts: 2,
		},
		{
			// TLS material only takes effect under an SSL protocol, so a CA path on a
			// plaintext connection must produce no TLS option.
			name:     "ca path is inert without an ssl protocol",
			sec:      Security{Protocol: ProtocolPlaintext, CALocation: "/nonexistent/ca.pem"},
			wantOpts: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			opts, err := tt.sec.Options()
			if err != nil {
				t.Fatalf("Options() error = %v", err)
			}
			if len(opts) != tt.wantOpts {
				t.Fatalf("Options() returned %d options, want %d", len(opts), tt.wantOpts)
			}
		})
	}
}

// TestSecuritySASLMechanismNames pins each configured mechanism to the wire name it
// negotiates.
func TestSecuritySASLMechanismNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mechanism string
		wantName  string
	}{
		{mechanism: MechanismPlain, wantName: "PLAIN"},
		{mechanism: MechanismScramSHA256, wantName: "SCRAM-SHA-256"},
		{mechanism: MechanismScramSHA512, wantName: "SCRAM-SHA-512"},
		// The spelling is normalised before dispatch, so a lowercase flag value must
		// reach the same mechanism rather than falling through to the error case.
		{mechanism: "scram-sha-512", wantName: "SCRAM-SHA-512"},
	}

	for _, tt := range tests {
		t.Run(tt.mechanism, func(t *testing.T) {
			t.Parallel()
			sec := Security{
				Protocol:      ProtocolSASLSSL,
				SASLMechanism: tt.mechanism,
				SASLUsername:  "u",
				SASLPassword:  "p",
			}
			m, err := sec.saslMechanism()
			if err != nil {
				t.Fatalf("saslMechanism() = %v", err)
			}
			if got := m.Name(); got != tt.wantName {
				t.Fatalf("saslMechanism(%q).Name() = %q, want %q", tt.mechanism, got, tt.wantName)
			}
		})
	}
}

func TestSecurityOptionsTLSFromPEMFiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	certPath := filepath.Join(dir, "client.pem")
	keyPath := filepath.Join(dir, "client.key")
	writeSelfSignedPair(t, certPath, keyPath)
	// The same self-signed certificate doubles as its own CA, which is all the loader
	// needs: it only has to parse and land in the pool.
	copyFile(t, certPath, caPath)

	t.Run("ca and mTLS pair load", func(t *testing.T) {
		sec := Security{Protocol: ProtocolSSL, CALocation: caPath, CertLocation: certPath, KeyLocation: keyPath}
		if err := sec.Validate(); err != nil {
			t.Fatalf("Validate() = %v", err)
		}
		cfg, err := sec.tlsConfig()
		if err != nil {
			t.Fatalf("tlsConfig() = %v", err)
		}
		if cfg.RootCAs == nil {
			t.Error("RootCAs is nil, want the CA file to have been loaded")
		}
		if len(cfg.Certificates) != 1 {
			t.Errorf("got %d client certificates, want 1", len(cfg.Certificates))
		}
		if cfg.MinVersion < 0x0303 { // TLS 1.2
			t.Errorf("MinVersion = %#x, want at least TLS 1.2", cfg.MinVersion)
		}
	})

	t.Run("missing ca file", func(t *testing.T) {
		sec := Security{Protocol: ProtocolSSL, CALocation: filepath.Join(dir, "absent.pem")}
		if _, err := sec.Options(); err == nil || !strings.Contains(err.Error(), "read CA certificate") {
			t.Fatalf("Options() = %v, want a read CA certificate error", err)
		}
	})

	t.Run("ca file with no certificates", func(t *testing.T) {
		// AppendCertsFromPEM reports failure with a bare bool; without the explicit
		// check this would produce an empty pool and only fail at handshake time.
		junk := filepath.Join(dir, "junk.pem")
		if err := os.WriteFile(junk, []byte("not a certificate\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		sec := Security{Protocol: ProtocolSSL, CALocation: junk}
		if _, err := sec.Options(); err == nil || !strings.Contains(err.Error(), "no valid PEM certificates") {
			t.Fatalf("Options() = %v, want a no valid PEM certificates error", err)
		}
	})

	t.Run("encrypted key is refused before any file is read", func(t *testing.T) {
		sec := Security{Protocol: ProtocolSSL, CertLocation: certPath, KeyLocation: keyPath, KeyPassword: "hunter2"}
		_, err := sec.Options()
		if err == nil {
			t.Fatal("Options() = nil, want the encrypted-key error")
		}
		for _, want := range []string{"--ssl-key-password", "encrypted private keys are not supported"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("Options() = %q, want it to contain %q", err, want)
			}
		}
	})
}

// writeSelfSignedPair writes a throwaway self-signed certificate and its unencrypted
// PKCS#8 key, in PEM, to the given paths.
func writeSelfSignedPair(t *testing.T, certPath, keyPath string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "khaos-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}

	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}

	writePEM(t, certPath, "CERTIFICATE", der)
	writePEM(t, keyPath, "PRIVATE KEY", keyDER)
}

func writePEM(t *testing.T, path, blockType string, der []byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	defer f.Close()
	if err := pem.Encode(f, &pem.Block{Type: blockType, Bytes: der}); err != nil {
		t.Fatalf("encode %s: %v", path, err)
	}
}

func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	b, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read %s: %v", src, err)
	}
	if err := os.WriteFile(dst, b, 0o600); err != nil {
		t.Fatalf("write %s: %v", dst, err)
	}
}
