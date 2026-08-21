package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Security protocols. Values are compared case-insensitively so that both the librdkafka
// spelling used on the CLI (SASL_SSL) and the lowercase Go spelling work.
const (
	ProtocolPlaintext     = "plaintext"
	ProtocolSSL           = "ssl"
	ProtocolSASLPlaintext = "sasl_plaintext"
	ProtocolSASLSSL       = "sasl_ssl"
)

// SASL mechanisms. This is the complete supported set; anything else is rejected before a
// client is built -- see Validate.
const (
	MechanismPlain       = "PLAIN"
	MechanismScramSHA256 = "SCRAM-SHA-256"
	MechanismScramSHA512 = "SCRAM-SHA-512"
)

// Security is the connection security configuration for a cluster.
//
// The TLS fields are file paths because that is what the CLI flags are named after
// (--ssl-ca-location etc.); Go has no equivalent of handing paths to the TLS stack, so
// Options loads and parses them itself.
type Security struct {
	// Protocol is one of plaintext, ssl, sasl_plaintext, sasl_ssl. Empty means plaintext.
	Protocol string

	// SASLMechanism is PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512.
	SASLMechanism string
	SASLUsername  string
	SASLPassword  string

	// CALocation is a PEM file of root CAs used to verify the broker. Empty means the
	// system trust store.
	CALocation string

	// CertLocation and KeyLocation are the client certificate and private key for mTLS.
	// Validate requires them to be set together.
	CertLocation string
	KeyLocation  string

	// KeyPassword is the passphrase for an encrypted KeyLocation. Not supported -- see
	// errEncryptedKeyUnsupported.
	KeyPassword string
}

// errEncryptedKeyUnsupported is returned when an encrypted private key is configured.
// crypto/x509.DecryptPEMBlock is deprecated and cannot read the PKCS#8
// EncryptedPrivateKeyInfo format that every modern tool (openssl genpkey, openssl pkcs8,
// cert-manager) produces, so this is rejected up front and by name rather than surfacing
// later as an opaque "failed to find any PEM data in key input" from tls.LoadX509KeyPair.
var errEncryptedKeyUnsupported = errors.New(
	"--ssl-key-password: encrypted private keys are not supported; " +
		"decrypt the key first with `openssl pkcs8 -topk8 -nocrypt -in enc.key -out plain.key` " +
		"and pass the decrypted file to --ssl-key-location")

// usesSASL reports whether the protocol requires SASL authentication.
func (s Security) usesSASL() bool {
	p := s.protocol()
	return p == ProtocolSASLPlaintext || p == ProtocolSASLSSL
}

// usesTLS reports whether the protocol requires a TLS transport.
func (s Security) usesTLS() bool {
	p := s.protocol()
	return p == ProtocolSSL || p == ProtocolSASLSSL
}

// protocol normalises Protocol, defaulting to plaintext. Lowercasing here means both the
// CLI's uppercase spelling and Go's lowercase constants are accepted.
func (s Security) protocol() string {
	if s.Protocol == "" {
		return ProtocolPlaintext
	}
	return strings.ToLower(s.Protocol)
}

// mechanism normalises SASLMechanism to the uppercase, hyphenated spelling.
func (s Security) mechanism() string {
	return strings.ToUpper(strings.TrimSpace(s.SASLMechanism))
}

// Validate reports whether the flag combination is coherent. It deliberately does not touch
// the filesystem: a missing certificate is a runtime failure reported by Options, not a
// configuration error -- Validate only answers "do these flags make sense together".
func (s Security) Validate() error {
	switch s.protocol() {
	case ProtocolPlaintext, ProtocolSSL, ProtocolSASLPlaintext, ProtocolSASLSSL:
	default:
		return fmt.Errorf("invalid security protocol %q: must be one of %s, %s, %s, %s",
			s.Protocol, ProtocolPlaintext, ProtocolSSL, ProtocolSASLPlaintext, ProtocolSASLSSL)
	}

	if m := s.mechanism(); m != "" {
		switch m {
		case MechanismPlain, MechanismScramSHA256, MechanismScramSHA512:
		default:
			return fmt.Errorf("invalid SASL mechanism %q: must be one of %s, %s, %s",
				s.SASLMechanism, MechanismPlain, MechanismScramSHA256, MechanismScramSHA512)
		}
	}

	// A SASL protocol requires a mechanism and both credentials.
	if s.usesSASL() {
		if s.mechanism() == "" {
			return errors.New("sasl_mechanism required when using SASL security protocol")
		}
		if s.SASLUsername == "" || s.SASLPassword == "" {
			return errors.New("sasl_username and sasl_password required for SASL")
		}
	}

	// The mTLS pair is all-or-nothing.
	if s.CertLocation != "" && s.KeyLocation == "" {
		return errors.New("ssl_key_location required when ssl_cert_location is provided")
	}
	if s.KeyLocation != "" && s.CertLocation == "" {
		return errors.New("ssl_cert_location required when ssl_key_location is provided")
	}

	// Rejected during validation so the CLI reports it alongside the other flag errors
	// instead of at connect time.
	if s.KeyPassword != "" {
		return errEncryptedKeyUnsupported
	}

	return nil
}

// Options returns the kgo options implementing this configuration, loading and parsing any
// TLS material rather than handing paths to a library. Returns an empty slice for plaintext
// with no credentials; Validate is called first, so a caller cannot skip it.
func (s Security) Options() ([]kgo.Opt, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}

	var opts []kgo.Opt

	if s.usesSASL() {
		m, err := s.saslMechanism()
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(m))
	}

	// Gated on the protocol rather than on whether TLS paths are set, so a CA path
	// configured alongside a plaintext protocol does not silently upgrade the
	// connection to TLS.
	if s.usesTLS() {
		tlsCfg, err := s.tlsConfig()
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	return opts, nil
}

// saslMechanism builds the franz-go SASL mechanism for the configured credentials.
func (s Security) saslMechanism() (sasl.Mechanism, error) {
	switch s.mechanism() {
	case MechanismPlain:
		return plain.Auth{User: s.SASLUsername, Pass: s.SASLPassword}.AsMechanism(), nil
	case MechanismScramSHA256:
		return scram.Auth{User: s.SASLUsername, Pass: s.SASLPassword}.AsSha256Mechanism(), nil
	case MechanismScramSHA512:
		return scram.Auth{User: s.SASLUsername, Pass: s.SASLPassword}.AsSha512Mechanism(), nil
	default:
		// Unreachable: Validate rejects anything else. Kept so a future mechanism added
		// to the constant list without a case here fails loudly instead of connecting
		// unauthenticated.
		return nil, fmt.Errorf("unsupported SASL mechanism %q", s.SASLMechanism)
	}
}

// tlsConfig builds a *tls.Config from the configured PEM files.
func (s Security) tlsConfig() (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}

	if s.CALocation != "" {
		pem, err := os.ReadFile(s.CALocation)
		if err != nil {
			return nil, fmt.Errorf("read CA certificate %q: %w", s.CALocation, err)
		}
		pool := x509.NewCertPool()
		// AppendCertsFromPEM reports failure with a bool and no detail, and skips
		// unparseable blocks silently. Without this check a typo'd or DER-encoded CA
		// file yields an empty pool and the far less obvious error
		// "x509: certificate signed by unknown authority" at handshake time.
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("CA certificate %q contains no valid PEM certificates", s.CALocation)
		}
		cfg.RootCAs = pool
	}

	// Validate guarantees these are both set or both empty.
	if s.CertLocation != "" {
		cert, err := tls.LoadX509KeyPair(s.CertLocation, s.KeyLocation)
		if err != nil {
			return nil, fmt.Errorf("load client key pair (cert %q, key %q): %w",
				s.CertLocation, s.KeyLocation, err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	return cfg, nil
}
