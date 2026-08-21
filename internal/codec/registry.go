package codec

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

// registryProbeTimeout bounds the reachability check in [NewRegistry].
const registryProbeTimeout = 10 * time.Second

// Registry is khaos's view of a Confluent Schema Registry: it fetches schemas
// for the `schema_provider: registry` path, registers the schemas khaos
// generates, and caches both. Caching matters because every topic builds its
// codec at startup, and a run with many topics would otherwise hammer the
// registry.
//
// A Registry is safe for concurrent use.
type Registry struct {
	client *sr.Client
	url    string
	// cfg is kept so that a 401/403 arriving mid-run -- an expired bearer token, an ACL
	// that only covers some subjects -- can be reported with the same "which flag is
	// wrong" detail as one arriving during the startup probe.
	cfg RegistryConfig

	mu         sync.Mutex
	subjects   map[string]*subjectSchema // by subject name
	registered map[registryKey]int       // by subject + schema text
}

// registryKey identifies a registration attempt. Both parts matter: the same
// text under two subjects is two registrations.
type registryKey struct {
	subject string
	text    string
}

// subjectSchema is one schema fetched from the registry, in all the forms
// khaos needs: the format tag, the field definitions the generator produces
// data from, the verbatim text the codec compiles, and the id the wire header
// carries.
type subjectSchema struct {
	format string // formatAvro or formatProtobuf
	fields []scenario.Field
	text   string
	id     int
}

// RegistryConfig is the Schema Registry endpoint and how to authenticate to
// it: basic auth, a bearer token, or TLS material let khaos reach a secured
// registry -- Confluent Cloud, Aiven, anything behind a proxy -- while a bare
// URL alone behaves as an unauthenticated client.
type RegistryConfig struct {
	URL string

	// Basic auth. On Confluent Cloud these are the Schema Registry API key and secret,
	// which are distinct from the Kafka cluster's SASL credentials.
	Username string
	Password string

	// BearerToken is sent as `Authorization: Bearer ...`, mutually exclusive with basic
	// auth.
	BearerToken string

	// TLS, for a registry behind a private CA or requiring a client certificate.
	CALocation   string
	CertLocation string
	KeyLocation  string
}

// Configured reports whether a registry is available at all.
func (c RegistryConfig) Configured() bool { return c.URL != "" }

// usesBasicAuth reports whether either half of the basic-auth pair was given. Either half
// alone is a mistake, which is why this is not `&&`.
func (c RegistryConfig) usesBasicAuth() bool { return c.Username != "" || c.Password != "" }

// usesTLSMaterial reports whether any TLS file was given. Any one of them implies the
// caller means to reach the registry over https.
func (c RegistryConfig) usesTLSMaterial() bool {
	return c.CALocation != "" || c.CertLocation != "" || c.KeyLocation != ""
}

// Validate reports whether the credential flags are coherent, without touching the network.
//
// Every check here catches a mistake that would otherwise surface as an opaque 401 or as a
// silently unencrypted connection, and it catches it at startup rather than on the first
// produce. The messages name CLI flags because that is what the user typed; the same
// spirit as kafka.Security.Validate, which rejects half a SASL or mTLS pair up front.
func (c RegistryConfig) Validate() error {
	if c.URL == "" {
		return errors.New("codec: schema registry url is empty")
	}

	// Preferring one silently would authenticate as somebody the user did not name, and
	// the resulting 403 would point nowhere.
	if c.usesBasicAuth() && c.BearerToken != "" {
		return errors.New("codec: --schema-registry-token cannot be combined with " +
			"--schema-registry-username/--schema-registry-password: a registry takes one " +
			"Authorization header, so pass either the token or the basic-auth pair")
	}

	// Half a pair is always a typo or a lost shell quote. sr.BasicAuth would happily send
	// `user:` and the registry would answer 401 with no clue which half was missing.
	if c.Username != "" && c.Password == "" {
		return errors.New("codec: --schema-registry-password is required with --schema-registry-username")
	}
	if c.Password != "" && c.Username == "" {
		return errors.New("codec: --schema-registry-username is required with --schema-registry-password")
	}

	// Same rule the broker connection applies to its own mTLS pair (internal/kafka:
	// Security.Validate).
	if c.CertLocation != "" && c.KeyLocation == "" {
		return errors.New("codec: --schema-registry-key-location is required with --schema-registry-cert-location")
	}
	if c.KeyLocation != "" && c.CertLocation == "" {
		return errors.New("codec: --schema-registry-cert-location is required with --schema-registry-key-location")
	}

	// sr.URLs prepends "http://" to a schemeless URL, and DialTLSConfig only ever applies
	// to an https request. So TLS material against an http (or schemeless) URL is not
	// merely redundant, it is ignored -- the client connects in the clear and the CA the
	// user carefully exported is never consulted. Say so instead.
	if c.usesTLSMaterial() && !strings.HasPrefix(strings.ToLower(c.URL), "https://") {
		return fmt.Errorf("codec: schema registry TLS files were given but the URL %q is not https; "+
			"a schemeless or http:// registry URL connects in the clear and the CA and client "+
			"certificate are ignored", c.URL)
	}

	return nil
}

// clientOpts turns the configuration into franz-go client options.
func (c RegistryConfig) clientOpts() ([]sr.ClientOpt, error) {
	opts := []sr.ClientOpt{sr.URLs(c.URL)}

	// Validate has already rejected both together, so the order of these cases is not a
	// silent preference.
	switch {
	case c.BearerToken != "":
		opts = append(opts, sr.BearerToken(c.BearerToken))
	case c.usesBasicAuth():
		opts = append(opts, sr.BasicAuth(c.Username, c.Password))
	}

	if c.usesTLSMaterial() {
		tlsCfg, err := c.tlsConfig()
		if err != nil {
			return nil, err
		}
		opts = append(opts, sr.DialTLSConfig(tlsCfg))
	}

	return opts, nil
}

// tlsConfig builds a *tls.Config from the configured PEM files.
//
// Deliberately identical in behaviour to the broker-side loader in internal/kafka
// (Security.tlsConfig): the same flags with a different prefix should fail the same way.
// That package's helper is unexported and this one must not import it, so the pattern is
// repeated rather than shared.
func (c RegistryConfig) tlsConfig() (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}

	if c.CALocation != "" {
		pem, err := os.ReadFile(c.CALocation)
		if err != nil {
			return nil, fmt.Errorf("codec: read schema registry CA certificate %q: %w", c.CALocation, err)
		}
		pool := x509.NewCertPool()
		// AppendCertsFromPEM reports failure with a bool and skips unparseable blocks
		// silently, so without this check a DER-encoded or truncated CA file yields an
		// empty pool and the far more confusing "x509: certificate signed by unknown
		// authority" at handshake time.
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("codec: schema registry CA certificate %q contains no valid PEM certificates", c.CALocation)
		}
		cfg.RootCAs = pool
	}

	// Validate guarantees these are both set or both empty.
	if c.CertLocation != "" {
		cert, err := tls.LoadX509KeyPair(c.CertLocation, c.KeyLocation)
		if err != nil {
			return nil, fmt.Errorf("codec: load schema registry client key pair (cert %q, key %q): %w",
				c.CertLocation, c.KeyLocation, err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	return cfg, nil
}

// authError rewrites a 401 or 403 from the registry into a message that names the URL and
// the flag most likely at fault, and returns nil for every other error.
//
// This is the point of the whole feature. franz-go surfaces a rejected request as
// `unable to GET "https://psrc-xxxx.confluent.cloud/subjects": 401 Unauthorized`, which
// tells a user nothing about which of the two similarly-named credential pairs on a
// Confluent Cloud page they pasted wrong. what describes the request in progress, so a
// subject-scoped ACL failure can be told apart from a wholesale rejection.
func (c RegistryConfig) authError(what string, err error) error {
	var re *sr.ResponseError
	if !errors.As(err, &re) {
		return nil
	}
	switch re.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
	default:
		return nil
	}
	return fmt.Errorf("codec: schema registry at %q rejected %s with HTTP %d %s: %s: %w",
		c.URL, what, re.StatusCode, http.StatusText(re.StatusCode), c.authHint(re.StatusCode), err)
}

// authHint is the "probably this flag" half of authError.
func (c RegistryConfig) authHint(status int) string {
	const confluentNote = "on Confluent Cloud these are the Schema Registry API key and secret, " +
		"which are NOT the Kafka cluster API key and secret"

	switch {
	case c.BearerToken != "":
		if status == http.StatusForbidden {
			return "the token in --schema-registry-token was accepted but is not allowed to do " +
				"this; check its role bindings, and that it was issued for THIS registry"
		}
		return "check --schema-registry-token: it may have expired or belong to another registry"
	case c.usesBasicAuth():
		if status == http.StatusForbidden {
			return "the credentials were accepted but are not allowed to do this; check the " +
				"subject-level ACLs, and that --schema-registry-username is a key for THIS " +
				"registry (" + confluentNote + ")"
		}
		return "check --schema-registry-username and --schema-registry-password: " + confluentNote
	default:
		if status == http.StatusForbidden {
			return "no credentials were sent; pass --schema-registry-username and " +
				"--schema-registry-password, or --schema-registry-token"
		}
		return "this registry requires authentication but no credentials were given; pass " +
			"--schema-registry-username and --schema-registry-password (" + confluentNote +
			"), or --schema-registry-token"
	}
}

// NewRegistry builds a registry client from a bare URL, with no authentication.
//
// This is what every caller got before credentials existed, and it must stay exactly
// equivalent: RegistryConfig with only a URL adds no client option at all.
func NewRegistry(url string) (*Registry, error) {
	return NewRegistryWithConfig(RegistryConfig{URL: url})
}

// NewRegistryWithConfig connects to a Schema Registry, authenticating as configured, and
// verifies it is reachable.
//
// The reachability probe runs over the same authenticated client used for real requests, so
// a bad URL or bad credentials become a startup error instead of surfacing mid-run on the
// first serialised message.
func NewRegistryWithConfig(cfg RegistryConfig) (*Registry, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	opts, err := cfg.clientOpts()
	if err != nil {
		return nil, err
	}
	url := cfg.URL
	client, err := sr.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("codec: schema registry client for %q: %w", url, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), registryProbeTimeout)
	defer cancel()
	if _, err := client.Subjects(ctx); err != nil {
		if authErr := cfg.authError("the startup reachability check", err); authErr != nil {
			return nil, authErr
		}
		return nil, fmt.Errorf("codec: schema registry at %q is unreachable: %w", url, err)
	}

	return &Registry{
		client:     client,
		url:        url,
		cfg:        cfg,
		subjects:   make(map[string]*subjectSchema),
		registered: make(map[registryKey]int),
	}, nil
}

// URL reports the registry address, for logs and error messages.
func (r *Registry) URL() string { return r.url }

// FieldsFor returns the data format and the field definitions of a subject's
// latest schema, so the generator can produce data that matches it. format is
// "avro" or "protobuf"; any other schema type is an error.
func (r *Registry) FieldsFor(ctx context.Context, subject string) (string, []scenario.Field, error) {
	s, err := r.fetch(ctx, subject)
	if err != nil {
		return "", nil, err
	}
	return s.format, s.fields, nil
}

// fetch returns the cached schema for a subject, fetching it on first use.
func (r *Registry) fetch(ctx context.Context, subject string) (*subjectSchema, error) {
	r.mu.Lock()
	cached, ok := r.subjects[subject]
	r.mu.Unlock()
	if ok {
		return cached, nil
	}

	ss, err := r.client.SchemaByVersion(ctx, subject, -1)
	if err != nil {
		// A subject the credentials may not read answers 403, not 404. Reporting that as
		// "schema not found" sends the user looking for a subject that is right there.
		if authErr := r.cfg.authError(fmt.Sprintf("the lookup of subject %q", subject), err); authErr != nil {
			return nil, authErr
		}
		return nil, fmt.Errorf("codec: schema not found for subject %q: %w", subject, err)
	}

	resolved := &subjectSchema{text: ss.Schema.Schema, id: ss.ID}
	switch ss.Schema.Type {
	case sr.TypeAvro:
		resolved.format = formatAvro
		resolved.fields, err = avroFieldsFromSchema(resolved.text)
	case sr.TypeProtobuf:
		resolved.format = formatProtobuf
		resolved.fields, err = protoFieldsFromSource(ctx, resolved.text)
	default:
		return nil, fmt.Errorf("codec: subject %q has unsupported schema type %s", subject, ss.Schema.Type)
	}
	if err != nil {
		return nil, fmt.Errorf("codec: subject %q: %w", subject, err)
	}

	r.mu.Lock()
	r.subjects[subject] = resolved
	r.mu.Unlock()
	return resolved, nil
}

// register ensures a generated schema exists under a subject and returns its
// id.
//
// franz-go's sr does not auto-register anything, so this is explicit. Posting a
// schema that is already registered is not an error: the registry normalises
// the text, finds the existing version and returns its id, so the
// already-registered case needs no special handling beyond the in-process cache
// that keeps khaos from re-posting the same text once per topic per run.
func (r *Registry) register(ctx context.Context, subject, text string, typ sr.SchemaType) (int, error) {
	key := registryKey{subject: subject, text: text}

	r.mu.Lock()
	id, ok := r.registered[key]
	r.mu.Unlock()
	if ok {
		return id, nil
	}

	ss, err := r.client.CreateSchema(ctx, subject, sr.Schema{Schema: text, Type: typ})
	if err != nil {
		// Read-only credentials are the common managed-cluster case: the fetch path works
		// and only registration is refused, so the hint has to reach here too.
		if authErr := r.cfg.authError(fmt.Sprintf("the registration of a %s schema under subject %q", typ, subject), err); authErr != nil {
			return 0, authErr
		}
		return 0, fmt.Errorf("codec: register %s schema for subject %q: %w", typ, subject, err)
	}

	r.mu.Lock()
	r.registered[key] = ss.ID
	r.mu.Unlock()
	return ss.ID, nil
}
